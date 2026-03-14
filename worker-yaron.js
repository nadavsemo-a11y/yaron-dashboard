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

        let allItems = [];
        let cursor = null;
        let hasMore = true;

        // Paginate through ALL items (500 per page)
        while (hasMore) {
          const itemsFragment = `
            items {
              id
              name
              column_values {
                id
                text
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

          if (page.cursor) {
            cursor = page.cursor;
          } else {
            hasMore = false;
          }
        }

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
        const supplierMap = {};
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
            phone: getCol('lookup_mkywf7pb'),       // טלפון לקוח (mirror)
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
                  parentInfo: parentInfo,
                  created_at: subitem.created_at,
                  status: status,
                  date: taskDate,
                  person: personColumn ? personColumn.text : '',
                  supplier: supplier,
                  supplierPhone: (supplier && supplierMap[supplier]) ? supplierMap[supplier].phone : '',
                });
              }
            }
          }
        }

        // Sorting is handled by the frontend

        return new Response(JSON.stringify(filteredTasks), {
          headers: { ...corsHeaders, 'Content-Type': 'application/json' },
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

      return new Response('Not Found', { status: 404, headers: corsHeaders });

    } catch (error) {
      return new Response(JSON.stringify({ error: error.message }), {
        status: 500,
        headers: { ...corsHeaders, 'Content-Type': 'application/json' },
      });
    }
  }
};
