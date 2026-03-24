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
              const supplierId = (supplierColumn && supplierColumn.linked_item_ids && supplierColumn.linked_item_ids.length > 0) ? supplierColumn.linked_item_ids[0] : '';

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
                  supplierId: supplierId,
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

      // GET /suppliers - List all suppliers with phone numbers
      if (url.pathname === '/suppliers' && request.method === 'GET') {
        try {
          const query = `query { boards(ids: [5089266595]) { items_page(limit: 100) { items { id name column_values(ids: ["phone_mkywgg4z"]) { text value } } } } }`;
          const res = await fetch('https://api.monday.com/v2', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json', 'Authorization': env.MONDAY_API_TOKEN, 'API-Version': '2024-10' },
            body: JSON.stringify({ query }),
          });
          const data = await res.json();
          const suppliers = (data.data?.boards?.[0]?.items_page?.items || []).map(s => {
            const phoneCol = s.column_values?.[0];
            let phone = phoneCol?.text || '';
            if (!phone && phoneCol?.value) { try { phone = JSON.parse(phoneCol.value).phone || ''; } catch {} }
            return { id: s.id, name: s.name, phone };
          });
          return new Response(JSON.stringify(suppliers), {
            headers: { ...corsHeaders, 'Content-Type': 'application/json' },
          });
        } catch (e) {
          return new Response(JSON.stringify({ error: e.message }), {
            status: 500, headers: { ...corsHeaders, 'Content-Type': 'application/json' },
          });
        }
      }

      // POST /assign-supplier - Assign a supplier to a subitem via board relation
      if (url.pathname === '/assign-supplier' && request.method === 'POST') {
        try {
          const body = await request.json();
          const { subitemId, supplierId } = body;
          if (!subitemId || !supplierId) {
            return new Response(JSON.stringify({ error: 'Missing subitemId or supplierId' }), {
              status: 400, headers: { ...corsHeaders, 'Content-Type': 'application/json' },
            });
          }

          // Set board relation on the subitem (link to supplier)
          const mutation = `mutation {
            change_column_value(
              board_id: ${env.MONDAY_SUBITEMS_BOARD_ID},
              item_id: ${subitemId},
              column_id: "board_relation_mkyw3bx3",
              value: ${JSON.stringify(JSON.stringify({ item_ids: [parseInt(supplierId)] }))}
            ) { id }
          }`;
          const res = await fetch('https://api.monday.com/v2', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json', 'Authorization': env.MONDAY_API_TOKEN, 'API-Version': '2024-10' },
            body: JSON.stringify({ query: mutation }),
          });
          const data = await res.json();
          if (data.errors) {
            return new Response(JSON.stringify({ success: false, error: data.errors }), {
              status: 500, headers: { ...corsHeaders, 'Content-Type': 'application/json' },
            });
          }
          return new Response(JSON.stringify({ success: true }), {
            headers: { ...corsHeaders, 'Content-Type': 'application/json' },
          });
        } catch (e) {
          return new Response(JSON.stringify({ success: false, error: e.message }), {
            status: 500, headers: { ...corsHeaders, 'Content-Type': 'application/json' },
          });
        }
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

      // ══════════════════════════════════════════════════════════════════
      // ADAPTIVE MATCHER — inlined from adaptive-matcher.js (canonical source)
      // ══════════════════════════════════════════════════════════════════

      // ── Default Config ──
      const AM_DEFAULT_CONFIG = {
        weights: { coreTokenOverlap: 0.25, weightedJaccard: 0.15, subsetScore: 0.10, tokenSortSimilarity: 0.15, rawEditSimilarity: 0.10, technicalAgreement: 0.05, substringBoost: 0.10 },
        penalties: { descriptorConflict: 0.15, numericConflict: 0.10 },
        boosts: { confirmedEntityBoost: 0.10 },
        thresholds: { auto: 85, autoGap: 20, candidate: 40, maxCandidates: 5 },
        noiseTokens: ['בהמתנה', 'ל', 'לט', 'ט', 'ממתין', 'הושלם', 'בוטל', 'פעיל', 'PV', 'PV1', 'PV2', 'PV3', 'PV4'],
        descriptorTokens: ['חדש', 'ישן', 'גג', 'קרקע', 'שדרוג', 'תוספת', 'מטבח', 'ספא', 'מלון'],
        conflictPairs: [['חדש','ישן'], ['גג','קרקע'], ['PV1','PV2'], ['PV2','PV3'], ['PV1','PV3']],
        mode: 'assisted',
      };

      // ── Layer A: Base Representation ──
      function amNormalize(s) { return String(s).replace(/[\s\u0027\u2018\u2019\u002D\u2013]/g, '').replace(/ג׳/g, 'ג'); }

      function amTokenize(s, cfg) {
        const noiseSet = new Set((cfg || AM_DEFAULT_CONFIG).noiseTokens);
        return String(s).replace(/ג׳/g, 'ג').replace(/[()[\]{}\-–—,.:;'"׳\/\\]/g, ' ')
          .split(/\s+/).map(t => t.trim()).filter(t => t.length > 0 && !noiseSet.has(t));
      }

      function amClassifyToken(token, allNames, cfg) {
        const c = cfg || AM_DEFAULT_CONFIG;
        if (new Set(c.noiseTokens).has(token)) return 'noise';
        if (new Set(c.descriptorTokens).has(token)) return 'descriptor';
        if (/^\d+([.,]\d+)?$/.test(token) || /^[\d.]+kw[ph]?$/i.test(token)) return 'technical';
        if (allNames && allNames.length > 0) {
          const norm = amNormalize(token);
          let count = 0;
          for (const n of allNames) { if (amNormalize(n).includes(norm)) count++; }
          if (count / allNames.length < 0.10) return 'identity';
        }
        return 'identity';
      }

      function amDocFrequency(allNames, cfg) {
        const freq = new Map();
        for (const name of allNames) {
          const tokens = amTokenize(name, cfg);
          const seen = new Set();
          for (const t of tokens) { const n = amNormalize(t); if (!seen.has(n)) { seen.add(n); freq.set(n, (freq.get(n) || 0) + 1); } }
        }
        return freq;
      }

      function amBuildRep(name, allNames, cfg) {
        const c = cfg || AM_DEFAULT_CONFIG;
        const tokens = amTokenize(name, c);
        const tokenClasses = {}, descriptors = [], technicalMarkers = [];
        for (const t of tokens) {
          const cls = amClassifyToken(t, allNames, c);
          tokenClasses[t] = cls;
          if (cls === 'descriptor') descriptors.push(t);
          if (cls === 'technical') technicalMarkers.push(t);
        }
        return { original: name, normalized: amNormalize(name), tokens, tokenClasses, tokenSorted: [...tokens].sort().join(' '), descriptors, technicalMarkers };
      }

      // ── Layer B: Candidate Ranking ──
      function amLevenshtein(a, b) {
        if (!a || !b) return 0;
        if (a === b) return 1;
        const la = a.length, lb = b.length;
        const m = Array.from({ length: la + 1 }, (_, i) => [i]);
        for (let j = 0; j <= lb; j++) m[0][j] = j;
        for (let i = 1; i <= la; i++)
          for (let j = 1; j <= lb; j++)
            m[i][j] = Math.min(m[i-1][j]+1, m[i][j-1]+1, m[i-1][j-1]+(a[i-1]===b[j-1]?0:1));
        return 1 - m[la][lb] / Math.max(la, lb);
      }

      function amTokenOverlap(tokensA, tokensB) {
        if (!tokensA.length || !tokensB.length) return 0;
        const nA = tokensA.map(amNormalize), nB = tokensB.map(amNormalize);
        let matches = 0;
        for (const ta of nA) {
          if (nB.includes(ta)) { matches++; continue; }
          for (const tb of nB) { if (tb.includes(ta) || ta.includes(tb)) { matches += 0.5; break; } }
        }
        return matches / Math.min(nA.length, nB.length);
      }

      function amWeightedJaccard(rA, rB, docFreq, totalDocs) {
        const setA = new Set(rA.tokens.map(amNormalize)), setB = new Set(rB.tokens.map(amNormalize));
        const union = new Set([...setA, ...setB]);
        if (!union.size) return 0;
        let interW = 0, unionW = 0;
        for (const t of union) {
          const w = Math.log(1 + (totalDocs || 1) / (docFreq ? (docFreq.get(t) || 1) : 1));
          unionW += w;
          if (setA.has(t) && setB.has(t)) interW += w;
        }
        return unionW > 0 ? interW / unionW : 0;
      }

      function amSubsetScore(rA, rB) {
        const nA = rA.tokens.map(amNormalize), nB = rB.tokens.map(amNormalize);
        if (!nA.length || !nB.length) return 0;
        let aInB = 0, bInA = 0;
        for (const t of nA) { if (nB.includes(t)) aInB++; }
        for (const t of nB) { if (nA.includes(t)) bInA++; }
        return Math.max(aInB / nA.length, bInA / nB.length);
      }

      function amTokenSortSim(rA, rB) { return amLevenshtein(amNormalize(rA.tokenSorted), amNormalize(rB.tokenSorted)); }
      function amRawEditSim(rA, rB) { return amLevenshtein(rA.normalized, rB.normalized); }

      function amTechAgreement(rA, rB) {
        const tA = rA.technicalMarkers, tB = rB.technicalMarkers;
        if (!tA.length && !tB.length) return 0.5;
        if (!tA.length || !tB.length) return 0;
        let m = 0; for (const t of tA) { if (tB.includes(t)) m++; }
        return m / Math.max(tA.length, tB.length);
      }

      function amSubstrBoost(rA, rB) { return (rA.normalized.includes(rB.normalized) || rB.normalized.includes(rA.normalized)) ? 1 : 0; }

      function amDescConflict(rA, rB, cfg) {
        for (const [d1, d2] of (cfg || AM_DEFAULT_CONFIG).conflictPairs) {
          const aH1 = rA.descriptors.includes(d1) || rA.tokens.includes(d1);
          const aH2 = rA.descriptors.includes(d2) || rA.tokens.includes(d2);
          const bH1 = rB.descriptors.includes(d1) || rB.tokens.includes(d1);
          const bH2 = rB.descriptors.includes(d2) || rB.tokens.includes(d2);
          if ((aH1 && bH2) || (aH2 && bH1)) return 1;
        }
        return 0;
      }

      function amNumConflict(rA, rB) {
        const nA = rA.tokens.filter(t => /^\d+$/.test(t)), nB = rB.tokens.filter(t => /^\d+$/.test(t));
        if (!nA.length || !nB.length) return 0;
        for (const a of nA) { for (const b of nB) { if (a !== b) return 1; } }
        return 0;
      }

      function amComputeFeatures(mRep, iRep, cfg, docFreq, totalDocs, entityClusters) {
        const coreOverlap = amTokenOverlap(mRep.tokens, iRep.tokens);
        const wJac = amWeightedJaccard(mRep, iRep, docFreq, totalDocs);
        const subset = amSubsetScore(mRep, iRep);
        const tokenSort = amTokenSortSim(mRep, iRep);
        const rawEdit = amRawEditSim(mRep, iRep);
        const tech = amTechAgreement(mRep, iRep);
        const substr = amSubstrBoost(mRep, iRep);
        const descC = amDescConflict(mRep, iRep, cfg);
        const numC = amNumConflict(mRep, iRep);
        let entityBoost = 0;
        if (entityClusters && entityClusters.length > 0) {
          for (const cl of entityClusters) {
            const vars = (cl.variants || []).map(amNormalize);
            if (vars.some(v => mRep.normalized.includes(v) || v.includes(mRep.normalized)) &&
                vars.some(v => iRep.normalized.includes(v) || v.includes(iRep.normalized))) { entityBoost = 1; break; }
          }
        }
        const normM = new Set(mRep.tokens.map(amNormalize)), normI = new Set(iRep.tokens.map(amNormalize));
        return {
          coreTokenOverlap: coreOverlap, weightedJaccard: wJac, subsetScore: subset,
          tokenSortSimilarity: tokenSort, rawEditSimilarity: rawEdit, technicalAgreement: tech,
          substringBoost: substr, descriptorConflict: descC, numericConflict: numC,
          confirmedEntityBoost: entityBoost,
          sharedTokens: [...normM].filter(t => normI.has(t)),
          mondayOnlyTokens: [...normM].filter(t => !normI.has(t)),
          intersolOnlyTokens: [...normI].filter(t => !normM.has(t)),
        };
      }

      function amComputeScore(features, cfg) {
        const c = cfg || AM_DEFAULT_CONFIG;
        const raw = c.weights.coreTokenOverlap * features.coreTokenOverlap +
          c.weights.weightedJaccard * features.weightedJaccard +
          c.weights.subsetScore * features.subsetScore +
          c.weights.tokenSortSimilarity * features.tokenSortSimilarity +
          c.weights.rawEditSimilarity * features.rawEditSimilarity +
          c.weights.technicalAgreement * features.technicalAgreement +
          c.weights.substringBoost * features.substringBoost -
          c.penalties.descriptorConflict * features.descriptorConflict -
          c.penalties.numericConflict * features.numericConflict +
          c.boosts.confirmedEntityBoost * features.confirmedEntityBoost;
        return Math.round(Math.max(0, Math.min(100, raw * 100)));
      }

      function amRankCandidates(mItem, entries, cfg, docFreq, totalDocs, entityClusters, knownBad) {
        const c = cfg || AM_DEFAULT_CONFIG;
        const allNames = entries.map(e => e.name);
        const mRep = amBuildRep(mItem.name, allNames, c);
        const results = [];
        for (const entry of entries) {
          if (knownBad && knownBad.has(`${mItem.name}|${entry.name}`)) continue;
          const iRep = amBuildRep(entry.name, allNames, c);
          const features = amComputeFeatures(mRep, iRep, c, docFreq, totalDocs, entityClusters);
          const score = amComputeScore(features, c);
          if (score >= c.thresholds.candidate) {
            results.push({
              intersolName: entry.name, intersolFields: entry.fields, intersolProj: entry.proj,
              score, features,
              scoreDetail: { token: Math.round(features.coreTokenOverlap * 100), levenshtein: Math.round(features.rawEditSimilarity * 100), substring: features.substringBoost > 0, combined: score },
            });
          }
        }
        results.sort((a, b) => b.score - a.score);
        return results.slice(0, c.thresholds.maxCandidates);
      }

      function amMatchAll(mondayItems, intersolEntries, cfg, opts) {
        const c = cfg || AM_DEFAULT_CONFIG;
        const o = opts || {};
        const knownBad = o.knownBad || new Set();
        const skipPat = o.skipPatterns || /תבנית|טסט|בדיקה|ניסויים/;
        const allNames = [...mondayItems.map(m => m.name), ...intersolEntries.map(e => e.name)];
        const docFreq = amDocFrequency(allNames, c);
        const totalDocs = allNames.length;
        const entityClusters = o.entityClusters || [];
        const auto = [], candidates = [];
        for (const mItem of mondayItems) {
          if (skipPat && skipPat.test(mItem.name)) continue;
          const ranked = amRankCandidates(mItem, intersolEntries, c, docFreq, totalDocs, entityClusters, knownBad);
          if (!ranked.length) continue;
          const topScore = ranked[0].score, secondScore = ranked.length > 1 ? ranked[1].score : 0, gap = topScore - secondScore;
          if (topScore >= c.thresholds.auto && (ranked.length === 1 || gap >= c.thresholds.autoGap)) {
            auto.push({ mondayId: mItem.id, mondayName: mItem.name, intersolName: ranked[0].intersolName, intersolFields: ranked[0].intersolFields, intersolProj: ranked[0].intersolProj, score: ranked[0].scoreDetail, features: ranked[0].features, rank: 1, candidateCount: ranked.length, marginFromNext: gap });
          } else {
            candidates.push({ mondayId: mItem.id, mondayName: mItem.name, matches: ranked.map((r, i) => ({ intersolName: r.intersolName, intersolFields: r.intersolFields, score: r.scoreDetail, features: r.features, rank: i + 1 })) });
          }
        }
        return { auto, candidates };
      }

      // ── Layer C: Feedback Store ──
      async function amRecordFeedback(kv, event) {
        if (!kv) return;
        const full = { timestamp: event.timestamp || Date.now(), mondayId: event.mondayId, mondayName: event.mondayName || '', intersolName: event.intersolName || '', decision: event.decision, score: event.score || 0, rank: event.rank || 0, candidateCount: event.candidateCount || 0, marginFromNext: event.marginFromNext || 0, features: event.features || {}, sharedTokens: event.sharedTokens || [], mondayOnlyTokens: event.mondayOnlyTokens || [], intersolOnlyTokens: event.intersolOnlyTokens || [] };
        let log = [];
        try { const raw = await kv.get('feedback:log'); if (raw) log = JSON.parse(raw); } catch {}
        log.push(full);
        if (log.length > 500) log = log.slice(log.length - 500);
        await kv.put('feedback:log', JSON.stringify(log));
      }

      async function amGetFeedbackHistory(kv, limit) {
        if (!kv) return [];
        try { const raw = await kv.get('feedback:log'); if (!raw) return []; const log = JSON.parse(raw); return (limit && limit > 0) ? log.slice(-limit) : log; } catch { return []; }
      }

      async function amLoadConfig(kv) {
        if (!kv) return { ...AM_DEFAULT_CONFIG };
        try {
          const raw = await kv.get('feedback:config');
          if (!raw) return { ...AM_DEFAULT_CONFIG };
          const stored = JSON.parse(raw);
          const result = {};
          for (const key of Object.keys(AM_DEFAULT_CONFIG)) {
            if (stored[key] !== undefined && typeof AM_DEFAULT_CONFIG[key] === 'object' && !Array.isArray(AM_DEFAULT_CONFIG[key])) {
              result[key] = { ...AM_DEFAULT_CONFIG[key], ...stored[key] };
            } else if (stored[key] !== undefined) { result[key] = stored[key]; } else { result[key] = AM_DEFAULT_CONFIG[key]; }
          }
          for (const key of Object.keys(stored)) { if (!(key in result)) result[key] = stored[key]; }
          return result;
        } catch { return { ...AM_DEFAULT_CONFIG }; }
      }

      async function amSaveConfig(kv, config) { if (kv) await kv.put('feedback:config', JSON.stringify(config)); }

      // ── Layer D: Calibration ──
      async function amCalibrate(kv) {
        if (!kv) return { success: false, error: 'No KV namespace' };
        let log = [];
        try { const raw = await kv.get('feedback:log'); if (raw) log = JSON.parse(raw); } catch {}
        if (log.length < 5) return { success: true, message: 'Insufficient feedback', events: log.length };

        const currentCfg = await amLoadConfig(kv);
        const WDMAX = 0.05, TDMAX = 2;

        // D1: Token stats
        const tokenStats = {};
        for (const ev of log) {
          const allTk = [...(ev.sharedTokens || []), ...(ev.mondayOnlyTokens || []), ...(ev.intersolOnlyTokens || [])];
          for (const tk of allTk) {
            if (!tokenStats[tk]) tokenStats[tk] = { approved: 0, rejected: 0, skipped: 0, total: 0, identityScore: 0, descriptorScore: 0, noiseScore: 0 };
            tokenStats[tk].total++;
            const isShared = (ev.sharedTokens || []).includes(tk);
            if (ev.decision === 'approved' || ev.decision === 'auto') { tokenStats[tk].approved++; tokenStats[tk][isShared ? 'identityScore' : 'descriptorScore'] += isShared ? 1 : 0.5; }
            else if (ev.decision === 'rejected') { tokenStats[tk].rejected++; if (isShared) tokenStats[tk].noiseScore += 0.5; }
            else if (ev.decision === 'skipped') { tokenStats[tk].skipped++; }
          }
        }
        for (const tk of Object.keys(tokenStats)) {
          const s = tokenStats[tk];
          const total = Math.max(s.identityScore + s.descriptorScore + s.noiseScore, 1);
          s.identityScore /= total; s.descriptorScore /= total; s.noiseScore /= total;
        }
        await kv.put('feedback:token_stats', JSON.stringify(tokenStats));

        // D2: Conflict pairs
        const pairCounts = {};
        for (const ev of log) {
          if (ev.decision !== 'rejected' && ev.decision !== 'approved' && ev.decision !== 'auto') continue;
          for (const mt of (ev.mondayOnlyTokens || [])) {
            for (const it of (ev.intersolOnlyTokens || [])) {
              const key = [mt, it].sort().join('|');
              if (!pairCounts[key]) pairCounts[key] = { rejected: 0, approved: 0 };
              pairCounts[key][ev.decision === 'rejected' ? 'rejected' : 'approved']++;
            }
          }
        }
        const existing = new Set((currentCfg.conflictPairs || []).map(p => [...p].sort().join('|')));
        const newPairs = [...(currentCfg.conflictPairs || [])];
        for (const [key, counts] of Object.entries(pairCounts)) {
          const total = counts.rejected + counts.approved;
          if (counts.rejected >= 3 && counts.rejected / total > 0.7 && !existing.has(key)) { newPairs.push(key.split('|')); existing.add(key); }
        }
        await kv.put('feedback:conflict_pairs', JSON.stringify(newPairs));

        // D3: Weights
        const approved = log.filter(e => e.decision === 'approved' || e.decision === 'auto');
        const rejected = log.filter(e => e.decision === 'rejected');
        const newW = { ...currentCfg.weights }, newP = { ...currentCfg.penalties }, newB = { ...currentCfg.boosts };

        if (approved.length >= 5 && rejected.length >= 2) {
          for (const fk of Object.keys(currentCfg.weights)) {
            const aM = approved.reduce((s, e) => s + ((e.features || {})[fk] || 0), 0) / approved.length;
            const rM = rejected.reduce((s, e) => s + ((e.features || {})[fk] || 0), 0) / rejected.length;
            newW[fk] = Math.max(0, Math.min(0.5, newW[fk] + Math.max(-WDMAX, Math.min(WDMAX, (aM - rM) * 0.1))));
          }
          for (const pk of Object.keys(currentCfg.penalties)) {
            const aM = approved.reduce((s, e) => s + ((e.features || {})[pk] || 0), 0) / approved.length;
            const rM = rejected.reduce((s, e) => s + ((e.features || {})[pk] || 0), 0) / rejected.length;
            newP[pk] = Math.max(0, Math.min(0.5, newP[pk] + Math.max(-WDMAX, Math.min(WDMAX, (rM - aM) * 0.1))));
          }
          for (const bk of Object.keys(currentCfg.boosts)) {
            const aM = approved.reduce((s, e) => s + ((e.features || {})[bk] || 0), 0) / approved.length;
            const rM = rejected.reduce((s, e) => s + ((e.features || {})[bk] || 0), 0) / rejected.length;
            newB[bk] = Math.max(0, Math.min(0.5, newB[bk] + Math.max(-WDMAX, Math.min(WDMAX, (aM - rM) * 0.1))));
          }
          const wSum = Object.values(newW).reduce((s, v) => s + v, 0) + Object.values(newP).reduce((s, v) => s + v, 0) + Object.values(newB).reduce((s, v) => s + v, 0);
          if (wSum > 0) { for (const k in newW) newW[k] /= wSum; for (const k in newP) newP[k] /= wSum; for (const k in newB) newB[k] /= wSum; }
        }

        // D4: Thresholds
        const newTh = { ...currentCfg.thresholds };
        if (approved.length >= 5) {
          const aScores = approved.map(e => e.score || 0).sort((a, b) => a - b);
          const ideal = aScores[Math.floor(aScores.length * 0.1)] || newTh.auto;
          newTh.auto = Math.round(Math.max(50, Math.min(95, newTh.auto + Math.max(-TDMAX, Math.min(TDMAX, ideal - newTh.auto)))));
          const idealC = Math.max(20, (aScores[Math.floor(aScores.length * 0.05)] || 40) - 10);
          newTh.candidate = Math.round(Math.max(20, Math.min(70, newTh.candidate + Math.max(-TDMAX, Math.min(TDMAX, idealC - newTh.candidate)))));
          const autoEvts = log.filter(e => e.decision === 'auto' && e.marginFromNext > 0);
          if (autoEvts.length >= 5) {
            const margins = autoEvts.map(e => e.marginFromNext).sort((a, b) => a - b);
            const p25 = margins[Math.floor(margins.length * 0.25)] || 20;
            newTh.autoGap = Math.round(Math.max(5, Math.min(40, newTh.autoGap + Math.max(-TDMAX, Math.min(TDMAX, p25 - newTh.autoGap)))));
          }
        }

        // D5: Entity clusters
        let existClusters = [];
        try { const raw = await kv.get('feedback:entity_clusters'); if (raw) existClusters = JSON.parse(raw); } catch {}
        const clusterMap = new Map();
        for (let i = 0; i < existClusters.length; i++) { for (const v of (existClusters[i].variants || [])) clusterMap.set(amNormalize(v), i); }
        for (const ev of approved) {
          const mN = amNormalize(ev.mondayName || ''), iN = amNormalize(ev.intersolName || '');
          if (!mN || !iN) continue;
          const idx = clusterMap.get(mN) ?? clusterMap.get(iN);
          if (idx !== undefined) {
            if (!existClusters[idx].variants.includes(ev.mondayName)) existClusters[idx].variants.push(ev.mondayName);
            if (!existClusters[idx].variants.includes(ev.intersolName)) existClusters[idx].variants.push(ev.intersolName);
            clusterMap.set(mN, idx); clusterMap.set(iN, idx);
          } else {
            const ni = existClusters.length;
            existClusters.push({ variants: [ev.mondayName, ev.intersolName], approvedCount: 1 });
            clusterMap.set(mN, ni); clusterMap.set(iN, ni);
          }
        }
        await kv.put('feedback:entity_clusters', JSON.stringify(existClusters));

        // D6: Dictionary suggestions
        const suggestions = { newNoise: [], newDescriptors: [] };
        for (const [tk, st] of Object.entries(tokenStats)) {
          if (st.total < 5) continue;
          if (st.noiseScore > 0.6 && st.identityScore < 0.2 && !currentCfg.noiseTokens.includes(tk)) suggestions.newNoise.push({ token: tk, noiseScore: st.noiseScore, total: st.total });
          if (st.descriptorScore > 0.5 && st.identityScore < 0.3 && !currentCfg.descriptorTokens.includes(tk) && !currentCfg.noiseTokens.includes(tk)) suggestions.newDescriptors.push({ token: tk, descriptorScore: st.descriptorScore, total: st.total });
        }

        const updatedCfg = { ...currentCfg, weights: newW, penalties: newP, boosts: newB, thresholds: newTh, conflictPairs: newPairs };
        await amSaveConfig(kv, updatedCfg);

        return { success: true, events: log.length, approved: approved.length, rejected: rejected.length, skipped: log.filter(e => e.decision === 'skipped').length, updatedWeights: newW, updatedPenalties: newP, updatedBoosts: newB, updatedThresholds: newTh, newConflictPairs: newPairs.length, entityClusters: existClusters.length, dictionarySuggestions: suggestions };
      }

      // ── Helper: extract INTERSOL fields ──
      function amExtractFields(proj) {
        const result = { kwp: proj.kwp, permit_limit: null, solar_module: null, solar_inverter: null, connection_size: null, project_name: proj.title };
        for (const a of ((proj.projectInfo || {}).assets || [])) {
          const v = a.value || '';
          if (typeof v === 'object') { if (v.project_name) result.project_name = v.project_name; if (v.permit_limit) result.permit_limit = v.permit_limit; if (v.connection_size) result.connection_size = v.connection_size; if (v.solar_module) result.solar_module = v.solar_module; if (v.solar_inverter) result.solar_inverter = v.solar_inverter; }
        }
        return result;
      }

      function amGetDesignPlanUrls(proj) {
        const dp = proj.designProgram;
        if (!dp || !dp.assets || !dp.assets.length) return [];
        const assets = dp.assets.filter(a => typeof a.value === 'string' && a.value.startsWith('http'));
        if (!assets.length) return [];
        if (assets.length === 1) return [{ label: assets[0].label, url: assets[0].value }];
        const vp = /^תכנון מפורט\s*(\d*)$/;
        const allV = assets.every(a => vp.test(a.label.trim()));
        if (allV) { let best = assets[0], bestN = 1; for (const a of assets) { const mt = a.label.trim().match(vp); const n = mt[1] ? parseInt(mt[1]) : 1; if (n > bestN) { bestN = n; best = a; } } return [{ label: best.label, url: best.value }]; }
        return assets.map(a => ({ label: a.label, url: a.value }));
      }

      // ══════════════════════════════════════════════════════════════════
      // ROUTES: INTERSOL Sync + Feedback + Config
      // ══════════════════════════════════════════════════════════════════

      // POST /intersol-sync - Adaptive matching + design plan PDF sync
      if (url.pathname === '/intersol-sync' && request.method === 'POST') {
        const INTERSOL_TOKEN_URL = 'https://admin.intersol-sv.com/wp-api/wp-json/jwt-auth/v1/token';
        const INTERSOL_PROJECTS_URL = 'https://admin.intersol-sv.com/wp-api/wp-json/projects/list';
        const INTERSOL_USER = 'SEMO AGS';
        const INTERSOL_PASS = 'ebFgSoP3Na!(XLX*1Alj4rWB';
        const DESIGN_PLAN_COL = 'file_mm1rdfhs';

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
          // Load adaptive config from KV
          const matchConfig = await amLoadConfig(env.TASKS_CACHE);

          // Load entity clusters for entity boost
          let entityClusters = [];
          try { const raw = await env.TASKS_CACHE.get('feedback:entity_clusters'); if (raw) entityClusters = JSON.parse(raw); } catch {}

          // Step 1: Login to INTERSOL
          const tokenRes = await fetch(INTERSOL_TOKEN_URL, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ username: INTERSOL_USER, password: INTERSOL_PASS }) });
          if (!tokenRes.ok) throw new Error('INTERSOL login failed');
          const token = (await tokenRes.json()).token;

          // Step 2: Fetch all INTERSOL projects
          const projRes = await fetch(INTERSOL_PROJECTS_URL, { headers: { 'Authorization': `Bearer ${token}`, 'Accept': 'application/json' } });
          if (!projRes.ok) throw new Error('INTERSOL fetch failed');
          const intersolProjects = (await projRes.json()).list || [];

          // Step 3: Fetch Monday items
          let mondayItems = [];
          let cursor = null;
          let hasMore = true;
          while (hasMore) {
            const q = cursor
              ? `query { boards(ids: ${env.MONDAY_BOARD_ID}) { items_page(limit: 500, cursor: "${cursor}") { cursor items { id name } } } }`
              : `query { boards(ids: ${env.MONDAY_BOARD_ID}) { items_page(limit: 500) { cursor items { id name } } } }`;
            const mRes = await fetch('https://api.monday.com/v2', { method: 'POST', headers: { 'Content-Type': 'application/json', 'Authorization': env.MONDAY_API_TOKEN, 'API-Version': '2024-10' }, body: JSON.stringify({ query: q }) });
            const mData = await mRes.json();
            const page = mData.data.boards[0].items_page;
            mondayItems = mondayItems.concat(page.items);
            cursor = page.cursor;
            hasMore = !!cursor;
          }

          // Step 4: Adaptive multi-candidate matching
          const intersolEntries = intersolProjects.map(proj => {
            const fields = amExtractFields(proj);
            return { proj, fields, name: fields.project_name || proj.title || '' };
          });

          const { auto: autoMatches, candidates } = amMatchAll(mondayItems, intersolEntries, matchConfig, {
            knownBad: KNOWN_BAD,
            entityClusters,
          });

          // Record auto-match feedback events
          for (const m of autoMatches) {
            await amRecordFeedback(env.TASKS_CACHE, {
              mondayId: m.mondayId, mondayName: m.mondayName, intersolName: m.intersolName,
              decision: 'auto', score: m.score.combined, rank: m.rank, candidateCount: m.candidateCount,
              marginFromNext: m.marginFromNext, features: m.features,
              sharedTokens: (m.features || {}).sharedTokens, mondayOnlyTokens: (m.features || {}).mondayOnlyTokens, intersolOnlyTokens: (m.features || {}).intersolOnlyTokens,
            });
          }

          // Step 5: Update Monday columns for auto matches
          let updated = 0;
          const errors = [];
          const batchParts = autoMatches.map(m => {
            const colValues = {};
            const f = m.intersolFields;
            if (f.solar_module) colValues[COLUMN_MAP.solar_module] = f.solar_module;
            if (f.solar_inverter) colValues[COLUMN_MAP.solar_inverter] = f.solar_inverter;
            if (f.kwp) colValues[COLUMN_MAP.max_dc] = String(f.kwp);
            if (f.kwp) colValues[COLUMN_MAP.kwp] = String(f.kwp);
            if (f.connection_size) colValues[COLUMN_MAP.connection_size] = f.connection_size;
            if (f.permit_limit) colValues[COLUMN_MAP.ac_power] = String(f.permit_limit);
            return { id: m.mondayId, name: m.mondayName, colValues };
          }).filter(b => Object.keys(b.colValues).length > 0);

          const BATCH_SIZE = 25;
          for (let i = 0; i < batchParts.length; i += BATCH_SIZE) {
            const batch = batchParts.slice(i, i + BATCH_SIZE);
            const mutations = batch.map((b, idx) =>
              `a${idx}: change_multiple_column_values(board_id: ${env.MONDAY_BOARD_ID}, item_id: ${b.id}, column_values: ${JSON.stringify(JSON.stringify(b.colValues))}) { id }`
            ).join('\n');
            try {
              const uRes = await fetch('https://api.monday.com/v2', { method: 'POST', headers: { 'Content-Type': 'application/json', 'Authorization': env.MONDAY_API_TOKEN, 'API-Version': '2024-10' }, body: JSON.stringify({ query: `mutation { ${mutations} }` }) });
              const uData = await uRes.json();
              if (uData.errors) batch.forEach(b => errors.push({ project: b.name, error: uData.errors }));
              else updated += batch.length;
            } catch (e) {
              batch.forEach(b => errors.push({ project: b.name, error: e.message }));
            }
          }

          // Step 6: Sync design plan PDFs (only changed files, KV cache)
          let designUploaded = 0, designSkipped = 0;
          const designErrors = [];

          for (const m of autoMatches) {
            const planUrls = amGetDesignPlanUrls(m.intersolProj);
            if (!planUrls.length) { designSkipped++; continue; }

            const cacheKey = `design_plan:${m.mondayId}`;
            const cached = await env.TASKS_CACHE.get(cacheKey);
            const cachedUrls = cached ? JSON.parse(cached) : [];
            const currentUrls = planUrls.map(p => p.url).sort();
            const cachedSorted = [...cachedUrls].sort();

            if (currentUrls.length === cachedSorted.length && currentUrls.every((u, i) => u === cachedSorted[i])) {
              designSkipped++;
              continue;
            }

            let allOk = true;
            for (const plan of planUrls) {
              try {
                const pdfRes = await fetch(plan.url);
                if (!pdfRes.ok) { designErrors.push({ project: m.mondayName, error: `PDF download failed: ${pdfRes.status}` }); allOk = false; break; }
                const pdfBlob = await pdfRes.blob();
                const query = `mutation ($file: File!) { add_file_to_column(item_id: ${m.mondayId}, column_id: "${DESIGN_PLAN_COL}", file: $file) { id } }`;
                const form = new FormData();
                form.append('query', query);
                form.append('map', JSON.stringify({ image: 'variables.file' }));
                form.append('image', new File([pdfBlob], `design_plan_${m.mondayId}.pdf`, { type: 'application/pdf' }));
                const upRes = await fetch('https://api.monday.com/v2/file', { method: 'POST', headers: { 'Authorization': env.MONDAY_API_TOKEN, 'API-Version': '2024-10' }, body: form });
                const upData = await upRes.json();
                if (upData.errors) { designErrors.push({ project: m.mondayName, error: upData.errors }); allOk = false; break; }
              } catch (e) { designErrors.push({ project: m.mondayName, error: e.message }); allOk = false; break; }
            }

            if (allOk) {
              await env.TASKS_CACHE.put(cacheKey, JSON.stringify(currentUrls));
              designUploaded++;
            }
          }

          // Run calibration if mode is 'auto'
          let calibrationResult = null;
          if (matchConfig.mode === 'auto') {
            calibrationResult = await amCalibrate(env.TASKS_CACHE);
          }

          return new Response(JSON.stringify({
            success: true,
            intersol_total: intersolProjects.length,
            monday_total: mondayItems.length,
            matched: autoMatches.length,
            updated,
            matcherMode: matchConfig.mode,
            designPlans: { uploaded: designUploaded, skipped: designSkipped, errors: designErrors.length > 0 ? designErrors : undefined },
            errors: errors.length > 0 ? errors : undefined,
            pendingMatches: candidates.length > 0 ? candidates : undefined,
            calibration: calibrationResult,
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

      // POST /apply-match - Apply a fuzzy match + record approved feedback
      if (url.pathname === '/apply-match' && request.method === 'POST') {
        try {
          const body = await request.json();
          const { mondayId, intersolFields, mondayName, intersolName, score, features, rank, candidateCount } = body;
          if (!mondayId || !intersolFields) {
            return new Response(JSON.stringify({ success: false, error: 'Missing mondayId or intersolFields' }), {
              status: 400, headers: { ...corsHeaders, 'Content-Type': 'application/json' },
            });
          }

          const COLUMN_MAP = {
            solar_module: 'text_mm1besx6',
            solar_inverter: 'text_mm1b2dx7',
            max_dc: 'numeric_mm1bdmv6',
            connection_size: 'text_mm1b1hq5',
            kwp: 'numeric_mkyw4dcb',
            ac_power: 'numeric_mkyxfrg9',
          };

          const colValues = {};
          if (intersolFields.solar_module) colValues[COLUMN_MAP.solar_module] = intersolFields.solar_module;
          if (intersolFields.solar_inverter) colValues[COLUMN_MAP.solar_inverter] = intersolFields.solar_inverter;
          if (intersolFields.kwp) colValues[COLUMN_MAP.max_dc] = String(intersolFields.kwp);
          if (intersolFields.kwp) colValues[COLUMN_MAP.kwp] = String(intersolFields.kwp);
          if (intersolFields.connection_size) colValues[COLUMN_MAP.connection_size] = intersolFields.connection_size;
          if (intersolFields.permit_limit) colValues[COLUMN_MAP.ac_power] = String(intersolFields.permit_limit);

          if (Object.keys(colValues).length > 0) {
            const mutation = `mutation { change_multiple_column_values(board_id: ${env.MONDAY_BOARD_ID}, item_id: ${mondayId}, column_values: ${JSON.stringify(JSON.stringify(colValues))}) { id } }`;
            const res = await fetch('https://api.monday.com/v2', {
              method: 'POST',
              headers: { 'Content-Type': 'application/json', 'Authorization': env.MONDAY_API_TOKEN, 'API-Version': '2024-10' },
              body: JSON.stringify({ query: mutation }),
            });
            const data = await res.json();
            if (data.errors) {
              return new Response(JSON.stringify({ success: false, error: data.errors }), {
                status: 500, headers: { ...corsHeaders, 'Content-Type': 'application/json' },
              });
            }
          }

          // Record "approved" feedback
          await amRecordFeedback(env.TASKS_CACHE, {
            mondayId, mondayName: mondayName || '', intersolName: intersolName || '',
            decision: 'approved', score: score || 0, rank: rank || 0,
            candidateCount: candidateCount || 0,
            features: features || {},
            sharedTokens: (features || {}).sharedTokens || [],
            mondayOnlyTokens: (features || {}).mondayOnlyTokens || [],
            intersolOnlyTokens: (features || {}).intersolOnlyTokens || [],
          });

          return new Response(JSON.stringify({ success: true, mondayId }), {
            headers: { ...corsHeaders, 'Content-Type': 'application/json' },
          });
        } catch (e) {
          return new Response(JSON.stringify({ success: false, error: e.message }), {
            status: 500, headers: { ...corsHeaders, 'Content-Type': 'application/json' },
          });
        }
      }

      // POST /intersol-feedback - Record a feedback event (skip/reject from frontend)
      if (url.pathname === '/intersol-feedback' && request.method === 'POST') {
        try {
          const body = await request.json();
          await amRecordFeedback(env.TASKS_CACHE, body);
          return new Response(JSON.stringify({ success: true }), {
            headers: { ...corsHeaders, 'Content-Type': 'application/json' },
          });
        } catch (e) {
          return new Response(JSON.stringify({ success: false, error: e.message }), {
            status: 500, headers: { ...corsHeaders, 'Content-Type': 'application/json' },
          });
        }
      }

      // GET /intersol-config - Return current adaptive config
      if (url.pathname === '/intersol-config' && request.method === 'GET') {
        try {
          const config = await amLoadConfig(env.TASKS_CACHE);
          const history = await amGetFeedbackHistory(env.TASKS_CACHE);
          return new Response(JSON.stringify({
            config,
            feedbackCount: history.length,
            approved: history.filter(e => e.decision === 'approved' || e.decision === 'auto').length,
            rejected: history.filter(e => e.decision === 'rejected').length,
            skipped: history.filter(e => e.decision === 'skipped').length,
          }), {
            headers: { ...corsHeaders, 'Content-Type': 'application/json' },
          });
        } catch (e) {
          return new Response(JSON.stringify({ error: e.message }), {
            status: 500, headers: { ...corsHeaders, 'Content-Type': 'application/json' },
          });
        }
      }

      // POST /intersol-calibrate - Manually trigger calibration
      if (url.pathname === '/intersol-calibrate' && request.method === 'POST') {
        try {
          const result = await amCalibrate(env.TASKS_CACHE);
          return new Response(JSON.stringify(result), {
            headers: { ...corsHeaders, 'Content-Type': 'application/json' },
          });
        } catch (e) {
          return new Response(JSON.stringify({ success: false, error: e.message }), {
            status: 500, headers: { ...corsHeaders, 'Content-Type': 'application/json' },
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
