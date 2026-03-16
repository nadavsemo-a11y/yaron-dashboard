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

      return new Response('Not Found', { status: 404, headers: corsHeaders });

    } catch (error) {
      return new Response(JSON.stringify({ error: error.message }), {
        status: 500,
        headers: { ...corsHeaders, 'Content-Type': 'application/json' },
      });
    }
  }
};
