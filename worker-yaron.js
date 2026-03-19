// Cloudflare Worker - Monday.com Integration for Yaron (with pagination)

export default {
  async fetch(request, env) {
    const corsHeaders = {
      'Access-Control-Allow-Origin': '*',
      'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
      'Access-Control-Allow-Headers': 'Content-Type',
    };

    if (request.method === 'OPTIONS') {
      return new Response(null, { headers: corsHeaders });
    }

    try {
      const url = new URL(request.url);

      // GET /items - Fetch Yaron's active subitems with pagination
      // ?all=true → return ALL users' tasks (not just Yaron), except completed
      if (url.pathname === '/items' && request.method === 'GET') {
        const showAll = url.searchParams.get('all') === 'true';
        const quickMode = url.searchParams.get('quick') === 'true';
        const noCache = url.searchParams.get('nocache') === 'true';
        const cacheKey = showAll ? 'items_all' : 'items_yaron';

        // Return cached data immediately if available
        if (!noCache && env.TASKS_CACHE) {
          const cached = await env.TASKS_CACHE.get(cacheKey);
          if (cached) {
            // Return cache immediately, refresh in background
            const ctx = typeof globalThis !== 'undefined' ? globalThis : null;
            if (!quickMode) {
              // Trigger background refresh (fire and forget via waitUntil if available)
            }
            return new Response(cached, {
              headers: { ...corsHeaders, 'Content-Type': 'application/json', 'X-Cache': 'HIT' },
            });
          }
        }

        let allItems = [];
        let cursor = null;
        let hasMore = true;

        // Paginate through items (quick=true → first page only)
        while (hasMore) {
          const itemsFragment = `
            items {
              id
              name
              column_values {
                id
                text
                value
                ... on BoardRelationValue {
                  linked_item_ids
                }
              }
              subitems {
                id
                name
                created_at
                column_values {
                  id
                  text
                  ... on BoardRelationValue {
                    display_value
                    linked_item_ids
                  }
                }
              }
            }
          `;
          const query = cursor
            ? `query { boards(ids: ${env.MONDAY_BOARD_ID}) { items_page(limit: 500, cursor: "${cursor}") { cursor ${itemsFragment} } } }`
            : `query { boards(ids: ${env.MONDAY_BOARD_ID}) { items_page(limit: 500) { cursor ${itemsFragment} } } }`;

          const response = await fetch('https://api.monday.com/v2', {
            method: 'POST',
            headers: {
              'Content-Type': 'application/json',
              'Authorization': env.MONDAY_API_TOKEN,
              'API-Version': '2024-10'
            },
            body: JSON.stringify({ query }),
          });

          const data = await response.json();

          if (data.errors) {
            return new Response(JSON.stringify({ error: 'Monday API Error', details: data.errors }), {
              status: 500,
              headers: { ...corsHeaders, 'Content-Type': 'application/json' },
            });
          }

          const page = data.data.boards[0].items_page;
          allItems = allItems.concat(page.items);

          if (page.cursor && !quickMode) {
            cursor = page.cursor;
          } else {
            hasMore = false;
          }
        }

        // In quick mode, skip supplier and client phone lookups for speed
        let supplierMap = {};
        let clientPhoneMap = {};

        if (!quickMode) {
        // Fetch suppliers with phone numbers (board 5089266595)
        const suppliersQuery = `
          query {
            boards(ids: 5089266595) {
              items_page(limit: 500) {
                items {
                  id
                  name
                  column_values(ids: ["phone_mkywgg4z"]) {
                    text
                    value
                  }
                }
              }
            }
          }
        `;
        const suppRes = await fetch('https://api.monday.com/v2', {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
            'Authorization': env.MONDAY_API_TOKEN,
            'API-Version': '2024-10'
          },
          body: JSON.stringify({ query: suppliersQuery }),
        });
        const suppData = await suppRes.json();
        supplierMap = {};
        if (suppData.data) {
          for (const s of suppData.data.boards[0].items_page.items) {
            const phoneCol = s.column_values[0];
            let phone = phoneCol ? phoneCol.text : '';
            if (!phone && phoneCol && phoneCol.value) {
              try { phone = JSON.parse(phoneCol.value).phone || ''; } catch {}
            }
            supplierMap[s.name] = { phone };
          }
        }

        // Fetch client phone numbers from clients board
        // Collect all client IDs from board_relation_mkywy46r
        const clientIds = new Set();
        for (const item of allItems) {
          const clientCol = item.column_values.find(c => c.id === 'board_relation_mkywy46r');
          if (clientCol && clientCol.linked_item_ids && clientCol.linked_item_ids.length > 0) {
            clientCol.linked_item_ids.forEach(id => clientIds.add(id));
          }
        }

        // Fetch phones for all clients in one query
        clientPhoneMap = {};
        if (clientIds.size > 0) {
          const idsArr = [...clientIds];
          // Batch in groups of 100
          for (let i = 0; i < idsArr.length; i += 100) {
            const batch = idsArr.slice(i, i + 100);
            const clientQuery = `query { items(ids: [${batch.join(',')}]) { id name column_values(ids: ["phone_mkyw1rbw"]) { text value } } }`;
            const cRes = await fetch('https://api.monday.com/v2', {
              method: 'POST',
              headers: {
                'Content-Type': 'application/json',
                'Authorization': env.MONDAY_API_TOKEN,
                'API-Version': '2024-10'
              },
              body: JSON.stringify({ query: clientQuery }),
            });
            const cData = await cRes.json();
            if (cData.data) {
              for (const ci of cData.data.items) {
                const ph = ci.column_values[0];
                let phone = ph ? ph.text : '';
                if (!phone && ph && ph.value) {
                  try { phone = JSON.parse(ph.value).phone || ''; } catch {}
                }
                clientPhoneMap[ci.id] = phone;
              }
            }
          }
        }
        } // end if (!quickMode)

        // Now filter subitems assigned to YARON SHOSHANA
        const filteredTasks = [];

        for (const item of allItems) {
          // Check parent project stage
          const parentStageColumn = item.column_values.find(
            col => col.id === env.MONDAY_STAGE_COLUMN_ID
          );
          const parentStage = parentStageColumn ? parentStageColumn.text : '';

          // Skip completed projects
          if (parentStage === 'הושלם') continue;

          // Extract technical info from parent project
          const getCol = (id) => {
            const col = item.column_values.find(c => c.id === id);
            if (!col || !col.text || col.text === 'None') return '';
            return col.text;
          };
          const parentInfo = {
            dc: getCol('numeric_mm1bdmv6'),        // הספק DC
            ac: getCol('numeric_mkyxfrg9'),         // הספק AC
            capacity: getCol('numeric_mkyw4dcb'),   // הספק (kWp)
            connectionSize: getCol('text_mm1b1hq5'),// גודל חיבור
            inverter: getCol('text_mm1b2dx7'),      // דגם ממיר
            panel: getCol('text_mm1besx6'),          // דגם פאנל
            roofType: getCol('dropdown_mkywtpq4'),  // סוג גג
            address: getCol('lookup_mkywmsse'),     // כתובת
            intersolLink: getCol('link_mm1k3v67'),  // לינק INTERSOL
            phone: (() => {
              const clientCol = item.column_values.find(c => c.id === 'board_relation_mkywy46r');
              if (clientCol && clientCol.linked_item_ids && clientCol.linked_item_ids.length > 0) {
                return clientPhoneMap[clientCol.linked_item_ids[0]] || '';
              }
              return '';
            })(),                                    // טלפון לקוח (מבורד לקוחות)
            stage: parentStage,                      // שלב
          };

          if (item.subitems && item.subitems.length > 0) {
            for (const subitem of item.subitems) {
              const personColumn = subitem.column_values.find(
                col => col.id === env.MONDAY_PERSON_COLUMN_ID
              );
              const statusColumn = subitem.column_values.find(
                col => col.id === env.MONDAY_STATUS_COLUMN_ID
              );

              const status = statusColumn ? statusColumn.text : '';
              const isYaron = personColumn && personColumn.text &&
                personColumn.text.toLowerCase().includes('yaron shoshana');

              // Include: assigned to Yaron (or all if showAll) + active statuses
              const dateColumn = subitem.column_values.find(
                col => col.id === 'timerange_mkywwz1t'
              );
              const dateRaw = dateColumn ? dateColumn.text : '';
              const taskDate = dateRaw ? dateRaw.split(' - ')[0] : null;

              // Get supplier name from subitem (board_relation uses display_value)
              const supplierColumn = subitem.column_values.find(
                col => col.id === 'board_relation_mkyw3bx3'
              );
              const supplier = supplierColumn ? (supplierColumn.display_value || supplierColumn.text || '') : '';

              const personMatch = showAll ? true : isYaron;
              if (personMatch && (status === 'ממתין' || status === 'בתהליך' || status === 'טרם החל')) {
                filteredTasks.push({
                  id: subitem.id,
                  name: subitem.name,
                  parentName: item.name,
                  parentId: item.id,
                  parentInfo: parentInfo,
                  created_at: subitem.created_at,
                  status: status,
                  date: taskDate,
                  person: personColumn ? personColumn.text : '',
                  supplier: supplier,
                  supplierPhone: (supplier && supplierMap[supplier]) ? supplierMap[supplier].phone : '',
                  hasClientLinked: (() => { const cc = item.column_values.find(c => c.id === 'board_relation_mkywy46r'); return !!(cc && cc.linked_item_ids && cc.linked_item_ids.length > 0); })(),
                });
              }
            }
          }
        }

        // Sorting is handled by the frontend
        const responseJson = JSON.stringify(filteredTasks);

        // Save to cache (expires in 5 minutes)
        if (env.TASKS_CACHE && !quickMode) {
          await env.TASKS_CACHE.put(cacheKey, responseJson, { expirationTtl: 300 });
        }

        return new Response(responseJson, {
          headers: { ...corsHeaders, 'Content-Type': 'application/json', 'X-Cache': 'MISS' },
        });
      }

      // POST /update-status - Update subitem status
      if (url.pathname === '/update-status' && request.method === 'POST') {
        const { itemId, newStatus } = await request.json();

        const validStatuses = ['ממתין', 'בתהליך', 'בוצע', 'טרם החל'];
        if (!validStatuses.includes(newStatus)) {
          return new Response(JSON.stringify({ error: 'Invalid status' }), {
            status: 400,
            headers: { ...corsHeaders, 'Content-Type': 'application/json' },
          });
        }

        const columnValue = JSON.stringify({ label: newStatus });

        const mutation = `
          mutation {
            change_column_value(
              board_id: ${env.MONDAY_SUBITEMS_BOARD_ID},
              item_id: ${itemId},
              column_id: "${env.MONDAY_STATUS_COLUMN_ID}",
              value: ${JSON.stringify(columnValue)}
            ) {
              id
            }
          }
        `;

        const response = await fetch('https://api.monday.com/v2', {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
            'Authorization': env.MONDAY_API_TOKEN,
            'API-Version': '2024-10'
          },
          body: JSON.stringify({ query: mutation }),
        });

        const data = await response.json();

        // Invalidate cache
        if (env.TASKS_CACHE) {
          await env.TASKS_CACHE.delete('items_yaron');
          await env.TASKS_CACHE.delete('items_all');
        }

        // Return full Monday response for debugging
        return new Response(JSON.stringify({
          success: !data.errors,
          monday_response: data
        }), {
          headers: { ...corsHeaders, 'Content-Type': 'application/json' },
        });
      }

      // POST /update-date - Update subitem date
      if (url.pathname === '/update-date' && request.method === 'POST') {
        const { itemId, date } = await request.json();
        // date format: "2026-03-11"
        const value = JSON.stringify({ from: date, to: date });

        const mutation = `
          mutation {
            change_column_value(
              board_id: ${env.MONDAY_SUBITEMS_BOARD_ID},
              item_id: ${itemId},
              column_id: "timerange_mkywwz1t",
              value: ${JSON.stringify(value)}
            ) {
              id
            }
          }
        `;

        const response = await fetch('https://api.monday.com/v2', {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
            'Authorization': env.MONDAY_API_TOKEN,
            'API-Version': '2024-10'
          },
          body: JSON.stringify({ query: mutation }),
        });

        const data = await response.json();

        if (data.errors) {
          return new Response(JSON.stringify({ error: 'Monday API Error', details: data.errors }), {
            status: 500,
            headers: { ...corsHeaders, 'Content-Type': 'application/json' },
          });
        }

        // Invalidate cache
        if (env.TASKS_CACHE) {
          await env.TASKS_CACHE.delete('items_yaron');
          await env.TASKS_CACHE.delete('items_all');
        }

        return new Response(JSON.stringify({ success: true, date }), {
          headers: { ...corsHeaders, 'Content-Type': 'application/json' },
        });
      }

      // GET /buttons - Fetch button configurations from Monday
      if (url.pathname === '/buttons' && request.method === 'GET') {
        const query = `
          query {
            boards(ids: ${env.MONDAY_BUTTONS_BOARD_ID}) {
              items_page(limit: 500) {
                items {
                  id
                  name
                  column_values {
                    id
                    text
                    value
                  }
                }
              }
            }
          }
        `;

        const response = await fetch('https://api.monday.com/v2', {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
            'Authorization': env.MONDAY_API_TOKEN,
            'API-Version': '2024-10'
          },
          body: JSON.stringify({ query }),
        });

        const data = await response.json();

        if (data.errors) {
          return new Response(JSON.stringify({ error: 'Monday API Error', details: data.errors }), {
            status: 500,
            headers: { ...corsHeaders, 'Content-Type': 'application/json' },
          });
        }

        const buttons = data.data.boards[0].items_page.items.map(item => {
          const getCol = (id) => {
            const col = item.column_values.find(c => c.id === id);
            return col ? col.text : '';
          };
          const getVal = (id) => {
            const col = item.column_values.find(c => c.id === id);
            if (!col || !col.value) return null;
            try { return JSON.parse(col.value); } catch { return null; }
          };

          // Extract phone - the phone column stores as JSON with phone and countryShortName
          const phoneVal = getVal('phone_mm1e9d07');
          const phone = phoneVal ? phoneVal.phone : getCol('phone_mm1e9d07');

          return {
            id: item.id,
            matchText: item.name,                       // שם הפריט = טקסט להתאמה
            buttonLabel: getCol('text_mm1e88wt'),       // טקסט כפתור
            phone: phone,                                // טלפון
            template: getCol('long_text_mm1ebfws'),     // תבנית הודעה
          };
        }).filter(b => b.matchText && b.buttonLabel);

        return new Response(JSON.stringify(buttons), {
          headers: { ...corsHeaders, 'Content-Type': 'application/json' },
        });
      }

      // POST /webhook-new-project - Auto-link client when project is created
      if (url.pathname === '/webhook-new-project' && request.method === 'POST') {
        const body = await request.json();

        // Monday sends a challenge for webhook verification
        if (body.challenge) {
          return new Response(JSON.stringify({ challenge: body.challenge }), {
            headers: { ...corsHeaders, 'Content-Type': 'application/json' },
          });
        }

        const event = body.event;
        if (!event || !event.pulseId) {
          return new Response(JSON.stringify({ ok: true }), {
            headers: { ...corsHeaders, 'Content-Type': 'application/json' },
          });
        }

        const projectId = event.pulseId;
        const projectName = event.pulseName || '';

        // Search for client in clients board (5089265844) by name
        const searchQuery = `
          query {
            boards(ids: 5089265844) {
              items_page(limit: 500, query_params: {rules: [{column_id: "name", compare_value: "${projectName.replace(/"/g, '\\"')}"}]}) {
                items {
                  id
                  name
                }
              }
            }
          }
        `;

        const searchRes = await fetch('https://api.monday.com/v2', {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
            'Authorization': env.MONDAY_API_TOKEN,
            'API-Version': '2024-10'
          },
          body: JSON.stringify({ query: searchQuery }),
        });

        const searchData = await searchRes.json();

        if (searchData.errors || !searchData.data) {
          // Fallback: search all clients and match by name
          const allClientsQuery = `
            query {
              boards(ids: 5089265844) {
                items_page(limit: 500) {
                  items { id name }
                }
              }
            }
          `;
          const allRes = await fetch('https://api.monday.com/v2', {
            method: 'POST',
            headers: {
              'Content-Type': 'application/json',
              'Authorization': env.MONDAY_API_TOKEN,
              'API-Version': '2024-10'
            },
            body: JSON.stringify({ query: allClientsQuery }),
          });
          const allData = await allRes.json();

          if (!allData.data) {
            return new Response(JSON.stringify({ success: false, error: 'Could not fetch clients' }), {
              headers: { ...corsHeaders, 'Content-Type': 'application/json' },
            });
          }

          const clients = allData.data.boards[0].items_page.items;
          // Find best match - project name contains client name or vice versa
          const projLower = projectName.toLowerCase().trim();
          let match = clients.find(c => c.name.toLowerCase().trim() === projLower);
          if (!match) {
            match = clients.find(c => projLower.includes(c.name.toLowerCase().trim()) || c.name.toLowerCase().trim().includes(projLower));
          }

          if (match) {
            // Link client to project
            const linkValue = JSON.stringify({ item_ids: [parseInt(match.id)] });
            const linkMutation = `
              mutation {
                change_column_value(
                  board_id: ${env.MONDAY_BOARD_ID},
                  item_id: ${projectId},
                  column_id: "board_relation_mkywy46r",
                  value: ${JSON.stringify(linkValue)}
                ) { id }
              }
            `;
            await fetch('https://api.monday.com/v2', {
              method: 'POST',
              headers: {
                'Content-Type': 'application/json',
                'Authorization': env.MONDAY_API_TOKEN,
                'API-Version': '2024-10'
              },
              body: JSON.stringify({ query: linkMutation }),
            });

            return new Response(JSON.stringify({ success: true, linked: match.name, clientId: match.id }), {
              headers: { ...corsHeaders, 'Content-Type': 'application/json' },
            });
          }

          return new Response(JSON.stringify({ success: true, linked: null, message: 'No matching client found' }), {
            headers: { ...corsHeaders, 'Content-Type': 'application/json' },
          });
        }

        // Use search results
        const results = searchData.data.boards[0].items_page.items;
        if (results.length > 0) {
          const match = results[0];
          const linkValue = JSON.stringify({ item_ids: [parseInt(match.id)] });
          const linkMutation = `
            mutation {
              change_column_value(
                board_id: ${env.MONDAY_BOARD_ID},
                item_id: ${projectId},
                column_id: "board_relation_mkywy46r",
                value: ${JSON.stringify(linkValue)}
              ) { id }
            }
          `;
          await fetch('https://api.monday.com/v2', {
            method: 'POST',
            headers: {
              'Content-Type': 'application/json',
              'Authorization': env.MONDAY_API_TOKEN,
              'API-Version': '2024-10'
            },
            body: JSON.stringify({ query: linkMutation }),
          });

          return new Response(JSON.stringify({ success: true, linked: match.name, clientId: match.id }), {
            headers: { ...corsHeaders, 'Content-Type': 'application/json' },
          });
        }

        return new Response(JSON.stringify({ success: true, linked: null, message: 'No matching client found' }), {
          headers: { ...corsHeaders, 'Content-Type': 'application/json' },
        });
      }

      // POST /create-client - Create client and link to project
      if (url.pathname === '/create-client' && request.method === 'POST') {
        const { parentItemId, clientName, phone } = await request.json();

        // Create client in clients board (5089265844)
        const createMutation = `mutation { create_item(board_id: 5089265844, item_name: "${clientName.replace(/"/g, '\\"')}") { id } }`;
        const createRes = await fetch('https://api.monday.com/v2', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json', 'Authorization': env.MONDAY_API_TOKEN, 'API-Version': '2024-10' },
          body: JSON.stringify({ query: createMutation }),
        });
        const createData = await createRes.json();

        if (createData.errors || !createData.data) {
          return new Response(JSON.stringify({ error: 'Failed to create client' }), {
            status: 500, headers: { ...corsHeaders, 'Content-Type': 'application/json' },
          });
        }

        const newClientId = createData.data.create_item.id;

        // Set phone number on the new client
        if (phone) {
          const phoneValue = JSON.stringify({ phone: phone, countryShortName: "IL" });
          const phoneMutation = `mutation { change_column_value(board_id: 5089265844, item_id: ${newClientId}, column_id: "phone_mkyw1rbw", value: ${JSON.stringify(phoneValue)}) { id } }`;
          await fetch('https://api.monday.com/v2', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json', 'Authorization': env.MONDAY_API_TOKEN, 'API-Version': '2024-10' },
            body: JSON.stringify({ query: phoneMutation }),
          });
        }

        // Link client to project
        const linkValue = JSON.stringify({ item_ids: [parseInt(newClientId)] });
        const linkMutation = `mutation { change_column_value(board_id: ${env.MONDAY_BOARD_ID}, item_id: ${parentItemId}, column_id: "board_relation_mkywy46r", value: ${JSON.stringify(linkValue)}) { id } }`;
        await fetch('https://api.monday.com/v2', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json', 'Authorization': env.MONDAY_API_TOKEN, 'API-Version': '2024-10' },
          body: JSON.stringify({ query: linkMutation }),
        });

        return new Response(JSON.stringify({ success: true, clientId: newClientId }), {
          headers: { ...corsHeaders, 'Content-Type': 'application/json' },
        });
      }

      // POST /spec/create - Create spec document and return short URL
      // Uses Monday data (parentInfo) — no INTERSOL connection needed
      if (url.pathname === '/spec/create' && request.method === 'POST') {
        const SEMO_WORKER_URL = 'https://s-a.gs';

        try {
          const body = await request.json();
          const { projectName, parentInfo, subitemId } = body;
          if (!projectName) {
            return new Response(JSON.stringify({ error: 'Missing projectName' }), {
              status: 400, headers: { ...corsHeaders, 'Content-Type': 'application/json' },
            });
          }

          const info = parentInfo || {};

          // Fetch design image from Monday subitem update (uploaded by INTERSOL sync)
          let designImageUrl = '';
          let imageDebug = {};
          if (subitemId) {
            try {
              const imgQuery = `query { items(ids: [${subitemId}]) { updates(limit: 20) { body assets { id url public_url } } } }`;
              const imgRes = await fetch('https://api.monday.com/v2', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json', 'Authorization': env.MONDAY_API_TOKEN, 'API-Version': '2024-10' },
                body: JSON.stringify({ query: imgQuery }),
              });
              const imgData = await imgRes.json();
              const updates = imgData.data?.items?.[0]?.updates || [];
              imageDebug.totalUpdates = updates.length;
              imageDebug.intersolUpdates = updates.filter(u => u.body && u.body.includes('INTERSOL')).length;
              for (const u of updates) {
                if (u.body && u.body.includes('INTERSOL') && u.assets && u.assets.length > 0) {
                  const asset = u.assets[0];
                  designImageUrl = asset.public_url || asset.url || '';
                  imageDebug.foundAsset = { id: asset.id, url: asset.url, public_url: asset.public_url };
                  break;
                }
              }
            } catch (e) { imageDebug.error = e.message; }
          }

          // Fallback: fetch from INTERSOL if Monday didn't have the image
          if (!designImageUrl && projectName) {
            try {
              const INTERSOL_TOKEN_URL = 'https://admin.intersol-sv.com/wp-api/wp-json/jwt-auth/v1/token';
              const INTERSOL_PROJECTS_URL = 'https://admin.intersol-sv.com/wp-api/wp-json/projects/list';
              const tokenRes = await fetch(INTERSOL_TOKEN_URL, {
                method: 'POST', headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ username: 'SEMO AGS', password: 'ebFgSoP3Na!(XLX*1Alj4rWB' }),
              });
              if (tokenRes.ok) {
                const token = (await tokenRes.json()).token;
                const projRes = await fetch(INTERSOL_PROJECTS_URL, {
                  headers: { 'Authorization': `Bearer ${token}`, 'Accept': 'application/json' },
                });
                if (projRes.ok) {
                  const projects = (await projRes.json()).list || [];
                  const normalize = (s) => s.replace(/[\s\u0027\u2018\u2019\u002D\u2013]/g, '').replace('ג׳', 'ג');
                  const mNorm = normalize(projectName);
                  for (const p of projects) {
                    const pName = ((p.projectInfo || {}).assets || []).reduce((n, a) => (typeof a.value === 'object' && a.value && a.value.project_name) ? a.value.project_name : n, p.title || '');
                    if (normalize(pName) === mNorm || mNorm.includes(normalize(pName)) || normalize(pName).includes(mNorm)) {
                      const di = p.designInfo;
                      if (di && di.assets && di.assets.length) {
                        designImageUrl = di.assets[di.assets.length - 1].value || '';
                        imageDebug.source = 'intersol';
                      }
                      break;
                    }
                  }
                }
              }
            } catch (e) { imageDebug.intersolError = e.message; }
          }

          // Calculate numModules from kwp and panel wattage
          let numModules = '';
          const panelStr = info.panel || '';
          const wattMatch = panelStr.match(/(\d{3,4})\s*[wW]/);
          const kwp = parseFloat(info.dc) || 0;
          if (wattMatch && kwp) {
            numModules = Math.round(kwp * 1000 / parseInt(wattMatch[1]));
          }

          const specData = {
            type: 'spec',
            subitemId: subitemId || '',
            projectName,
            address: info.address || '',
            kwp: info.dc || '',
            acPower: info.ac || '',
            connectionSize: info.connectionSize || '',
            numModules,
            solarModule: info.panel || '',
            solarInverter: info.inverter || '',
            designImageUrl,
            intersolUrl: info.intersolLink || '',
          };

          // Save to KV via s-a.gs worker
          const saveRes = await fetch(`${SEMO_WORKER_URL}/q/save`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ data: JSON.stringify(specData) }),
          });
          const saveResult = await saveRes.json();

          if (!saveResult.id) throw new Error('Failed to save spec data');

          return new Response(JSON.stringify({
            success: true,
            id: saveResult.id,
            url: saveResult.url,
            specData,
            imageDebug,
          }), {
            headers: { ...corsHeaders, 'Content-Type': 'application/json' },
          });

        } catch (e) {
          return new Response(JSON.stringify({ success: false, error: e.message }), {
            status: 500, headers: { ...corsHeaders, 'Content-Type': 'application/json' },
          });
        }
      }

      // POST /intersol-sync - Scan INTERSOL and update Monday.com
      if (url.pathname === '/intersol-sync' && request.method === 'POST') {
        const INTERSOL_TOKEN_URL = 'https://admin.intersol-sv.com/wp-api/wp-json/jwt-auth/v1/token';
        const INTERSOL_PROJECTS_URL = 'https://admin.intersol-sv.com/wp-api/wp-json/projects/list';
        const INTERSOL_USER = 'SEMO AGS';
        const INTERSOL_PASS = 'ebFgSoP3Na!(XLX*1Alj4rWB';

        const COLUMN_MAP = {
          solar_module: 'text_mm1besx6',
          solar_inverter: 'text_mm1b2dx7',
          max_dc: 'numeric_mm1bdmv6',
          connection_size: 'text_mm1b1hq5',
          kwp: 'numeric_mkyw4dcb',
          ac_power: 'numeric_mkyxfrg9',
        };

        const KNOWN_BAD = new Set(['שוקי -סנדרין|שוקי חזן', 'יקיר יהב|חיים יהב']);

        try {
          // Step 1: Login to INTERSOL
          const tokenRes = await fetch(INTERSOL_TOKEN_URL, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ username: INTERSOL_USER, password: INTERSOL_PASS }),
          });
          if (!tokenRes.ok) throw new Error('INTERSOL login failed');
          const tokenData = await tokenRes.json();
          const token = tokenData.token;

          // Step 2: Fetch all projects
          const projRes = await fetch(INTERSOL_PROJECTS_URL, {
            headers: { 'Authorization': `Bearer ${token}`, 'Accept': 'application/json' },
          });
          if (!projRes.ok) throw new Error('INTERSOL fetch failed');
          const projData = await projRes.json();
          const intersolProjects = projData.list || projData;

          // Step 3: Fetch Monday projects
          let mondayItems = [];
          let cursor = null;
          let hasMore = true;
          while (hasMore) {
            const q = cursor
              ? `query { boards(ids: ${env.MONDAY_BOARD_ID}) { items_page(limit: 500, cursor: "${cursor}") { cursor items { id name } } } }`
              : `query { boards(ids: ${env.MONDAY_BOARD_ID}) { items_page(limit: 500) { cursor items { id name } } } }`;
            const mRes = await fetch('https://api.monday.com/v2', {
              method: 'POST',
              headers: { 'Content-Type': 'application/json', 'Authorization': env.MONDAY_API_TOKEN, 'API-Version': '2024-10' },
              body: JSON.stringify({ query: q }),
            });
            const mData = await mRes.json();
            const page = mData.data.boards[0].items_page;
            mondayItems = mondayItems.concat(page.items);
            cursor = page.cursor;
            hasMore = !!cursor;
          }

          // Step 4: Match and update
          const normalize = (s) => s.replace(/[\s\u0027\u2018\u2019\u002D\u2013]/g, '').replace('ג׳', 'ג');

          // Extract INTERSOL fields helper
          function extractFields(proj) {
            const result = { kwp: proj.kwp, permit_limit: null, solar_module: null, solar_inverter: null, connection_size: null, project_name: proj.title };
            const assets = (proj.projectInfo || {}).assets || [];
            for (const a of assets) {
              const label = a.label || '';
              const value = a.value || '';
              if (typeof value === 'object') {
                if (value.project_name) result.project_name = value.project_name;
                if (value.permit_limit) result.permit_limit = value.permit_limit;
                if (value.connection_size) result.connection_size = value.connection_size;
                if (value.solar_module) result.solar_module = value.solar_module;
                if (value.solar_inverter) result.solar_inverter = value.solar_inverter;
              }
            }
            return result;
          }

          let updated = 0;
          let matched = 0;
          const errors = [];

          for (const mItem of mondayItems) {
            const mNorm = normalize(mItem.name);
            let bestMatch = null;
            let bestScore = 0;

            for (const iProj of intersolProjects) {
              const iFields = extractFields(iProj);
              const iName = iFields.project_name || iProj.title || '';
              const iNorm = normalize(iName);

              if (KNOWN_BAD.has(`${mItem.name}|${iName}`)) continue;

              if (mNorm === iNorm) { bestMatch = { proj: iProj, fields: iFields, name: iName }; break; }
              if (mNorm.includes(iNorm) || iNorm.includes(mNorm)) {
                const score = Math.min(mNorm.length, iNorm.length) / Math.max(mNorm.length, iNorm.length) * 90;
                if (score > bestScore) { bestMatch = { proj: iProj, fields: iFields, name: iName }; bestScore = score; }
              }
            }

            if (!bestMatch) continue;
            matched++;

            const f = bestMatch.fields;
            const colValues = {};
            if (f.solar_module) colValues[COLUMN_MAP.solar_module] = f.solar_module;
            if (f.solar_inverter) colValues[COLUMN_MAP.solar_inverter] = f.solar_inverter;
            if (f.kwp) colValues[COLUMN_MAP.max_dc] = String(f.kwp);
            if (f.kwp) colValues[COLUMN_MAP.kwp] = String(f.kwp);
            if (f.connection_size) colValues[COLUMN_MAP.connection_size] = f.connection_size;
            if (f.permit_limit) colValues[COLUMN_MAP.ac_power] = String(f.permit_limit);

            if (Object.keys(colValues).length === 0) continue;

            // Build batch mutation parts (multiple updates in one API call)
            if (!batchParts) var batchParts = [];
            batchParts.push({ id: mItem.id, name: mItem.name, colValues });
          }

          // Execute updates in batches of 10 (single API call each with aliases)
          const BATCH_SIZE = 10;
          for (let i = 0; i < (batchParts || []).length; i += BATCH_SIZE) {
            const batch = batchParts.slice(i, i + BATCH_SIZE);
            const mutations = batch.map((b, idx) =>
              `a${idx}: change_multiple_column_values(board_id: ${env.MONDAY_BOARD_ID}, item_id: ${b.id}, column_values: ${JSON.stringify(JSON.stringify(b.colValues))}) { id }`
            ).join('\n');

            try {
              const uRes = await fetch('https://api.monday.com/v2', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json', 'Authorization': env.MONDAY_API_TOKEN, 'API-Version': '2024-10' },
                body: JSON.stringify({ query: `mutation { ${mutations} }` }),
              });
              const uData = await uRes.json();
              if (uData.errors) {
                batch.forEach(b => errors.push({ project: b.name, error: uData.errors }));
              } else {
                updated += batch.length;
              }
            } catch (e) {
              batch.forEach(b => errors.push({ project: b.name, error: e.message }));
            }
          }

          return new Response(JSON.stringify({
            success: true,
            intersol_total: intersolProjects.length,
            monday_total: mondayItems.length,
            matched,
            updated,
            errors: errors.length > 0 ? errors : undefined,
          }), {
            headers: { ...corsHeaders, 'Content-Type': 'application/json' },
          });

        } catch (e) {
          return new Response(JSON.stringify({ success: false, error: e.message }), {
            status: 500,
            headers: { ...corsHeaders, 'Content-Type': 'application/json' },
          });
        }
      }

      // POST /intersol-media-sync - Upload design images + shortened share links
      // Processes in chunks of 10 to avoid subrequests limit
      // Body: { offset: 0 } (optional, defaults to 0)
      if (url.pathname === '/intersol-media-sync' && request.method === 'POST') {
        const INTERSOL_TOKEN_URL = 'https://admin.intersol-sv.com/wp-api/wp-json/jwt-auth/v1/token';
        const INTERSOL_PROJECTS_URL = 'https://admin.intersol-sv.com/wp-api/wp-json/projects/list';
        const INTERSOL_USER = 'SEMO AGS';
        const INTERSOL_PASS = 'ebFgSoP3Na!(XLX*1Alj4rWB';
        const SHORTENER_URL = 'https://s-a.gs/q/shorten';
        const INTERSOL_BASE = 'https://app.intersol-sv.com';
        const LINK_COLUMN = 'link_mm1k3v67';
        const CHUNK_SIZE = 10;

        const KNOWN_BAD = new Set(['שוקי -סנדרין|שוקי חזן', 'יקיר יהב|חיים יהב']);

        try {
          let body = {};
          try { body = await request.json(); } catch {}
          const offset = body.offset || 0;

          // Step 1: Login to INTERSOL
          const tokenRes = await fetch(INTERSOL_TOKEN_URL, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ username: INTERSOL_USER, password: INTERSOL_PASS }),
          });
          if (!tokenRes.ok) throw new Error('INTERSOL login failed');
          const token = (await tokenRes.json()).token;

          // Step 2: Fetch all INTERSOL projects
          const projRes = await fetch(INTERSOL_PROJECTS_URL, {
            headers: { 'Authorization': `Bearer ${token}`, 'Accept': 'application/json' },
          });
          if (!projRes.ok) throw new Error('INTERSOL fetch failed');
          const intersolProjects = ((await projRes.json()).list || []);

          // Step 3: Fetch Monday projects WITH subitems
          let mondayItems = [];
          let cursor = null;
          let hasMore = true;
          while (hasMore) {
            const q = cursor
              ? `query { boards(ids: ${env.MONDAY_BOARD_ID}) { items_page(limit: 500, cursor: "${cursor}") { cursor items { id name subitems { id name } } } } }`
              : `query { boards(ids: ${env.MONDAY_BOARD_ID}) { items_page(limit: 500) { cursor items { id name subitems { id name } } } } }`;
            const mRes = await fetch('https://api.monday.com/v2', {
              method: 'POST',
              headers: { 'Content-Type': 'application/json', 'Authorization': env.MONDAY_API_TOKEN, 'API-Version': '2024-10' },
              body: JSON.stringify({ query: q }),
            });
            const mData = await mRes.json();
            const page = mData.data.boards[0].items_page;
            mondayItems = mondayItems.concat(page.items);
            cursor = page.cursor;
            hasMore = !!cursor;
          }

          // Step 4: Match projects (same logic as /intersol-sync)
          const normalize = (s) => s.replace(/[\s\u0027\u2018\u2019\u002D\u2013]/g, '').replace('ג׳', 'ג');

          function getIntersolName(proj) {
            const assets = (proj.projectInfo || {}).assets || [];
            for (const a of assets) {
              if (typeof a.value === 'object' && a.value && a.value.project_name) return a.value.project_name;
            }
            return proj.title || '';
          }
          function getDesignImage(proj) {
            const di = proj.designInfo;
            if (!di || !di.assets || !di.assets.length) return null;
            return di.assets[di.assets.length - 1].value || null;
          }

          const matches = [];
          for (const mItem of mondayItems) {
            const mNorm = normalize(mItem.name);

            let bestMatch = null;
            let bestScore = 0;

            for (const iProj of intersolProjects) {
              const iName = getIntersolName(iProj);
              if (KNOWN_BAD.has(`${mItem.name}|${iName}`)) continue;
              const iNorm = normalize(iName);

              if (mNorm === iNorm) { bestMatch = iProj; bestScore = 100; break; }
              if (mNorm.includes(iNorm) || iNorm.includes(mNorm)) {
                const score = Math.min(mNorm.length, iNorm.length) / Math.max(mNorm.length, iNorm.length) * 90;
                if (score > bestScore) { bestMatch = iProj; bestScore = score; }
              }
            }

            if (bestMatch) {
              const designImage = getDesignImage(bestMatch);
              const shareUrl = `${INTERSOL_BASE}/projects/${bestMatch.id}/${bestMatch.slug || ''}`;
              const planSubitem = (mItem.subitems || []).find(s => s.name.includes('תכנון') && !s.name.includes('סופי'));
              matches.push({ mondayId: mItem.id, mondayName: mItem.name, intersolName: getIntersolName(bestMatch), designImage, shareUrl, subitemId: planSubitem ? planSubitem.id : null, hasDesignInfo: !!(bestMatch.designInfo && bestMatch.designInfo.assets), score: bestScore });
            }
          }

          // Step 5: Process chunk
          const chunk = matches.slice(offset, offset + CHUNK_SIZE);
          let linksUpdated = 0;
          let imagesUploaded = 0;
          let imagesSkipped = 0;
          const errors = [];
          // Debug: show first 5 INTERSOL names for verification
          const iSample = intersolProjects.slice(0, 5).map(p => ({ title: p.title, extractedName: getIntersolName(p), hasDI: !!(p.designInfo && p.designInfo.assets) }));
          const debug = chunk.map(m => ({ name: m.mondayName, iName: m.intersolName, hasImage: !!m.designImage, hasSubitem: !!m.subitemId, hasDI: m.hasDesignInfo, score: m.score }));

          for (const m of chunk) {
            // 5a: Shorten share URL and update link column
            if (m.shareUrl) {
              try {
                const shortRes = await fetch(SHORTENER_URL, {
                  method: 'POST',
                  headers: { 'Content-Type': 'application/json' },
                  body: JSON.stringify({ url: m.shareUrl }),
                });
                const shortData = await shortRes.json();
                const shortUrl = shortData.url || m.shareUrl;

                const linkValue = JSON.stringify({ url: shortUrl, text: 'הדמייה' });
                const mutation = `mutation { change_column_value(board_id: ${env.MONDAY_BOARD_ID}, item_id: ${m.mondayId}, column_id: "${LINK_COLUMN}", value: ${JSON.stringify(linkValue)}) { id } }`;
                const uRes = await fetch('https://api.monday.com/v2', {
                  method: 'POST',
                  headers: { 'Content-Type': 'application/json', 'Authorization': env.MONDAY_API_TOKEN, 'API-Version': '2024-10' },
                  body: JSON.stringify({ query: mutation }),
                });
                const uData = await uRes.json();
                if (!uData.errors) linksUpdated++;
                else errors.push({ project: m.mondayName, type: 'link', error: uData.errors });
              } catch (e) {
                errors.push({ project: m.mondayName, type: 'link', error: e.message });
              }
            }

            // 5b: Upload design image to subitem "תכנון + הצגה ללקוח"
            if (m.designImage && m.subitemId) {
              try {
                // Check if image was already uploaded (look for existing update with "INTERSOL")
                const checkQuery = `query { items(ids: [${m.subitemId}]) { updates(limit: 20) { id body assets { id } } } }`;
                const checkRes = await fetch('https://api.monday.com/v2', {
                  method: 'POST',
                  headers: { 'Content-Type': 'application/json', 'Authorization': env.MONDAY_API_TOKEN, 'API-Version': '2024-10' },
                  body: JSON.stringify({ query: checkQuery }),
                });
                const checkData = await checkRes.json();
                const existingUpdates = checkData.data?.items?.[0]?.updates || [];
                const alreadyHasImage = existingUpdates.some(u => u.body && u.body.includes('INTERSOL') && u.assets && u.assets.length > 0);

                if (alreadyHasImage) {
                  imagesSkipped++;
                  continue;
                }

                // Download image from INTERSOL
                const imgRes = await fetch(m.designImage, {
                  headers: { 'Authorization': `Bearer ${token}` },
                });
                if (!imgRes.ok) throw new Error(`Image download failed: ${imgRes.status}`);
                const imgBlob = await imgRes.blob();

                // Create update on subitem
                const createUpdateMut = `mutation { create_update(item_id: ${m.subitemId}, body: "תמונת הדמייה מ-INTERSOL") { id } }`;
                const updateRes = await fetch('https://api.monday.com/v2', {
                  method: 'POST',
                  headers: { 'Content-Type': 'application/json', 'Authorization': env.MONDAY_API_TOKEN, 'API-Version': '2024-10' },
                  body: JSON.stringify({ query: createUpdateMut }),
                });
                const updateData = await updateRes.json();
                if (updateData.errors) throw new Error(JSON.stringify(updateData.errors));
                const updateId = updateData.data.create_update.id;

                // Upload file to the update
                const ext = m.designImage.split('.').pop().split('?')[0] || 'jpg';
                const formData = new FormData();
                formData.append('query', `mutation ($file: File!) { add_file_to_update(update_id: ${updateId}, file: $file) { id } }`);
                formData.append('map', '{"image":"variables.file"}');
                formData.append('image', imgBlob, `design.${ext}`);

                const uploadRes = await fetch('https://api.monday.com/v2/file', {
                  method: 'POST',
                  headers: { 'Authorization': env.MONDAY_API_TOKEN },
                  body: formData,
                });
                const uploadData = await uploadRes.json();
                if (!uploadData.errors) imagesUploaded++;
                else errors.push({ project: m.mondayName, type: 'image', error: uploadData.errors });
              } catch (e) {
                errors.push({ project: m.mondayName, type: 'image', error: e.message });
              }
            }
          }

          const hasMore2 = offset + CHUNK_SIZE < matches.length;
          const noSubitem = chunk.filter(m => !m.subitemId).map(m => m.mondayName);
          const noImage = chunk.filter(m => !m.designImage).map(m => m.mondayName);

          return new Response(JSON.stringify({
            success: true,
            total_matches: matches.length,
            processed_offset: offset,
            processed_count: chunk.length,
            links_updated: linksUpdated,
            images_uploaded: imagesUploaded,
            images_skipped: imagesSkipped,
            has_more: hasMore2,
            next_offset: hasMore2 ? offset + CHUNK_SIZE : null,
            skipped_no_subitem: noSubitem.length > 0 ? noSubitem : undefined,
            skipped_no_image: noImage.length > 0 ? noImage : undefined,
            errors: errors.length > 0 ? errors : undefined,
            debug,
            intersol_sample: iSample,
          }), {
            headers: { ...corsHeaders, 'Content-Type': 'application/json' },
          });

        } catch (e) {
          return new Response(JSON.stringify({ success: false, error: e.message }), {
            status: 500,
            headers: { ...corsHeaders, 'Content-Type': 'application/json' },
          });
        }
      }

      return new Response('Not Found', { status: 404, headers: corsHeaders });

    } catch (error) {
      return new Response(JSON.stringify({ error: error.message }), {
        status: 500,
        headers: { ...corsHeaders, 'Content-Type': 'application/json' },
      });
    }
  }
};
