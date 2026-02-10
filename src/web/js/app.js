/*
-------------------------------------------------------
Shared state
-------------------------------------------------------
*/

let currentJob = null;

/*
Status helper
*/
function setStatus(msg) {
    document.getElementById("status").innerText = msg;
}

/*
-------------------------------------------------------
Preview Table
-------------------------------------------------------
*/

async function preview() {

    setStatus("Fetching table...");

    let url = document.getElementById("url").value;

    let res = await fetch("/preview", {
        method: "POST",
        headers: {"Content-Type": "application/json"},
        body: JSON.stringify({url})
    });

    let data = await res.json();

    showSchema(data);
    showRows(data);

    setStatus("Preview ready");
}

/*
Display inferred schema
*/
function showSchema(data) {
    let box = document.getElementById("schema");
    box.innerText = "";

    for (let c of data.columns) {
        box.innerText += c + " : " + data.types[c] + "\n";
    }
}

/*
Preview rows with formatted display
*/
function showRows(data) {

    let table = document.getElementById("rows");
    table.innerHTML = "";

    let head = "<tr>";
    data.columns.forEach(c => {
        head += "<th style='text-transform: capitalize;'>" + c.replace(/_/g, ' ') + "</th>";
    });
    head += "</tr>";

    table.innerHTML += head;

    data.rows.slice(0, 10).forEach(r => {
        let row = "<tr>";
        r.forEach((v, idx) => {
            let displayValue = v;

            // Format numbers nicely if they're numeric
            let type = data.types[data.columns[idx]];
            if ((type === 'INT' || type === 'FLOAT') && !isNaN(v) && v !== '' && v > 999) {
                displayValue = Number(v.replace(/[,$]/g, '')).toLocaleString();
            }

            row += "<td>" + displayValue + "</td>";
        });
        row += "</tr>";

        table.innerHTML += row;
    });
}

/*
-------------------------------------------------------
Ingestion Pipeline
-------------------------------------------------------
*/

async function ingest() {

    setStatus("Submitting ingestion...");

    let payload = {
        url: document.getElementById("url").value,
        table: document.getElementById("table").value,
        mode: document.getElementById("mode").value,
        dedup: document.getElementById("dedup").checked
    };

    let res = await fetch("/ingest", {
        method: "POST",
        headers: {"Content-Type": "application/json"},
        body: JSON.stringify(payload)
    });

    currentJob = await res.text();

    pollJob();
    pollLogs();
}

/*
Poll ingestion progress
*/
async function pollJob() {

    if (!currentJob) return;

    let res = await fetch("/job_status?id=" + currentJob);
    let s = await res.json();

    let percent = 0;
    if (s.total > 0)
        percent = (s.inserted / s.total) * 100;

    document.getElementById("progressBar")
        .style.width = percent + "%";

    document.getElementById("jobStatus")
        .innerText =
        `Inserted ${s.inserted} / ${s.total}
Status: ${s.status}`;

    if (s.status !== "completed")
        setTimeout(pollJob, 1000);
}

/*
Poll ingestion logs
*/
async function pollLogs() {

    if (!currentJob) return;

    let res = await fetch("/job_logs?id=" + currentJob);
    let logs = await res.json();

    let text = "";

    logs.reverse().forEach(l => {
        text += `[${l.time}] ${l.msg}\n`;
    });

    document.getElementById("logs").innerText = text;

    setTimeout(pollLogs, 1500);
}

/*
-------------------------------------------------------
Database Explorer
-------------------------------------------------------
*/

async function loadTables() {

    let res = await fetch("/tables");
    let tables = await res.json();

    let box = document.getElementById("tables");
    box.innerHTML = "";

    tables.forEach(t => {
        box.innerHTML += `
      <div class="tableItem"
           onclick="loadTable('${t}',this)">
        ${t}
      </div>`;
    });
}

async function loadTable(name, el) {

    document.querySelectorAll(".tableItem")
        .forEach(e => e.classList.remove("active"));

    if (el) el.classList.add("active");

    document.getElementById("tableTitle")
        .innerText = "Table: " + name;

    let res = await fetch("/table?name=" + name);
    let rows = await res.json();

    renderRows(rows);
}

/*
Properly decode MySQL values - handle byte arrays and nulls
*/
// function decodeValue(v){
//   if (v === null || v === undefined) return '';
//
//   // Rare case: buffer still slips through
//   // if (v && v.type === 'Buffer' && Array.isArray(v.data)) {
//   //   return new TextDecoder().decode(new Uint8Array(v.data));
//   // }
//
//   return v; // keep native type
// }



/*
Render DB rows with proper formatting
*/
function renderRows(rows) {
  const table = document.getElementById("dataView");
  table.innerHTML = "";

  if (!rows || rows.length === 0) {
    const tr = document.createElement("tr");
    const td = document.createElement("td");
    td.colSpan = 100;
    td.style.textAlign = "center";
    td.style.padding = "20px";
    td.textContent = "No data available";
    tr.appendChild(td);
    table.appendChild(tr);
    return;
  }

  const columns = Object.keys(rows[0]);

  // Header
  const headerRow = document.createElement("tr");
  columns.forEach(col => {
    const th = document.createElement("th");
    th.textContent = col.replace(/_/g, " ");
    th.style.textTransform = "capitalize";
    headerRow.appendChild(th);
  });
  table.appendChild(headerRow);

  // Data rows
  rows.forEach(r => {
    const tr = document.createElement("tr");

    columns.forEach(col => {
      const td = document.createElement("td");
      let v = r[col];

      if (typeof v === "number" && v > 999) {
        v = v.toLocaleString();
      }

      if (v === null || v === undefined) v = "";

      td.textContent = v;
      tr.appendChild(td);
    });

    table.appendChild(tr);
  });
}
