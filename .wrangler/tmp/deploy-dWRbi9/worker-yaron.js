// worker-yaron.js
var worker_yaron_default = {
  async fetch(request, env) {
    const corsHeaders = {
      "Access-Control-Allow-Origin": "*",
      "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
      "Access-Control-Allow-Headers": "Content-Type"
    };
    if (request.method === "OPTIONS") {
      return new Response(null, { headers: corsHeaders });
    }
    try {
      const url = new URL(request.url);
      if (url.pathname === "/items" && request.method === "GET") {
        let allItems = [];
        let cursor = null;
        let hasMore = true;
        while (hasMore) {
          const query = cursor ? `
              query {
                boards(ids: ${env.MONDAY_BOARD_ID}) {
                  items_page(limit: 500, cursor: "${cursor}") {
                    cursor
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
                        }
                      }
                    }
                  }
                }
              }
            ` : `
              query {
                boards(ids: ${env.MONDAY_BOARD_ID}) {
                  items_page(limit: 500) {
                    cursor
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
                        }
                      }
                    }
                  }
                }
              }
            `;
          const response = await fetch("https://api.monday.com/v2", {
            method: "POST",
            headers: {
              "Content-Type": "application/json",
              "Authorization": env.MONDAY_API_TOKEN,
              "API-Version": "2024-10"
            },
            body: JSON.stringify({ query })
          });
          const data = await response.json();
          if (data.errors) {
            return new Response(JSON.stringify({ error: "Monday API Error", details: data.errors }), {
              status: 500,
              headers: { ...corsHeaders, "Content-Type": "application/json" }
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
        const filteredTasks = [];
        for (const item of allItems) {
          const parentStageColumn = item.column_values.find(
            (col) => col.id === env.MONDAY_STAGE_COLUMN_ID
          );
          const parentStage = parentStageColumn ? parentStageColumn.text : "";
          if (parentStage === "\u05D4\u05D5\u05E9\u05DC\u05DD") continue;
          if (item.subitems && item.subitems.length > 0) {
            for (const subitem of item.subitems) {
              const personColumn = subitem.column_values.find(
                (col) => col.id === env.MONDAY_PERSON_COLUMN_ID
              );
              const statusColumn = subitem.column_values.find(
                (col) => col.id === env.MONDAY_STATUS_COLUMN_ID
              );
              const status = statusColumn ? statusColumn.text : "";
              const isYaron = personColumn && personColumn.text && personColumn.text.toLowerCase().includes("yaron shoshana");
              const dateColumn = subitem.column_values.find(
                (col) => col.id === "timerange_mkywwz1t"
              );
              const dateRaw = dateColumn ? dateColumn.text : "";
              const taskDate = dateRaw ? dateRaw.split(" - ")[0] : null;
              if (isYaron && (status === "\u05DE\u05DE\u05EA\u05D9\u05DF" || status === "\u05D1\u05EA\u05D4\u05DC\u05D9\u05DA" || status === "\u05D8\u05E8\u05DD \u05D4\u05D7\u05DC")) {
                filteredTasks.push({
                  id: subitem.id,
                  name: subitem.name,
                  parentName: item.name,
                  created_at: subitem.created_at,
                  status,
                  date: taskDate
                });
              }
            }
          }
        }
        filteredTasks.sort((a, b) => {
          const projectCompare = a.parentName.localeCompare(b.parentName, "he");
          if (projectCompare !== 0) return projectCompare;
          return a.name.localeCompare(b.name, "he");
        });
        return new Response(JSON.stringify(filteredTasks), {
          headers: { ...corsHeaders, "Content-Type": "application/json" }
        });
      }
      if (url.pathname === "/update-status" && request.method === "POST") {
        const { itemId, newStatus } = await request.json();
        const statusMap = {
          "\u05DE\u05DE\u05EA\u05D9\u05DF": 0,
          "\u05D1\u05EA\u05D4\u05DC\u05D9\u05DA": 2,
          "\u05D1\u05D5\u05E6\u05E2": 1,
          "\u05D8\u05E8\u05DD \u05D4\u05D7\u05DC": 3
        };
        const statusIndex = statusMap[newStatus];
        if (statusIndex === void 0) {
          return new Response(JSON.stringify({ error: "Invalid status" }), {
            status: 400,
            headers: { ...corsHeaders, "Content-Type": "application/json" }
          });
        }
        const mutation = `
          mutation {
            change_simple_column_value(
              board_id: ${env.MONDAY_SUBITEMS_BOARD_ID},
              item_id: ${itemId},
              column_id: "${env.MONDAY_STATUS_COLUMN_ID}",
              value: "${statusIndex}"
            ) {
              id
            }
          }
        `;
        const response = await fetch("https://api.monday.com/v2", {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
            "Authorization": env.MONDAY_API_TOKEN,
            "API-Version": "2024-10"
          },
          body: JSON.stringify({ query: mutation })
        });
        const data = await response.json();
        if (data.errors) {
          return new Response(JSON.stringify({ error: "Monday API Error", details: data.errors }), {
            status: 500,
            headers: { ...corsHeaders, "Content-Type": "application/json" }
          });
        }
        return new Response(JSON.stringify({ success: true }), {
          headers: { ...corsHeaders, "Content-Type": "application/json" }
        });
      }
      if (url.pathname === "/update-date" && request.method === "POST") {
        const { itemId, date } = await request.json();
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
        const response = await fetch("https://api.monday.com/v2", {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
            "Authorization": env.MONDAY_API_TOKEN,
            "API-Version": "2024-10"
          },
          body: JSON.stringify({ query: mutation })
        });
        const data = await response.json();
        if (data.errors) {
          return new Response(JSON.stringify({ error: "Monday API Error", details: data.errors }), {
            status: 500,
            headers: { ...corsHeaders, "Content-Type": "application/json" }
          });
        }
        return new Response(JSON.stringify({ success: true, date }), {
          headers: { ...corsHeaders, "Content-Type": "application/json" }
        });
      }
      return new Response("Not Found", { status: 404, headers: corsHeaders });
    } catch (error) {
      return new Response(JSON.stringify({ error: error.message }), {
        status: 500,
        headers: { ...corsHeaders, "Content-Type": "application/json" }
      });
    }
  }
};
export {
  worker_yaron_default as default
};
//# sourceMappingURL=worker-yaron.js.map
