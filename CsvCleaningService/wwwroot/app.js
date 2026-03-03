let state = {
  importId: null,
  fileName: null,
  suggestedRules: []
};

const csvFile = document.getElementById("csvFile");
const uploadBtn = document.getElementById("uploadBtn");
const applyBtn = document.getElementById("applyBtn");
const downloadCsvBtn = document.getElementById("downloadCsvBtn");
const downloadJsonBtn = document.getElementById("downloadJsonBtn");

const statusEl = document.getElementById("status");
const summarySection = document.getElementById("summarySection");
const transformSection = document.getElementById("transformSection");
const previewSection = document.getElementById("previewSection");

uploadBtn.addEventListener("click", uploadCsv);
applyBtn.addEventListener("click", applyRules);
downloadCsvBtn.addEventListener("click", () => download("csv"));
downloadJsonBtn.addEventListener("click", () => download("json"));

async function uploadCsv() {
  const file = csvFile.files?.[0];
  if (!file) {
    setStatus("Please select a CSV file.");
    return;
  }

  const formData = new FormData();
  formData.append("file", file);

  setStatus("Uploading and profiling...");
  const res = await fetch("/api/import/upload", {
    method: "POST",
    body: formData
  });

  const data = await res.json();
  if (!res.ok) {
    setStatus(data.error || "Upload failed.");
    return;
  }

  state.importId = data.importId;
  state.fileName = data.fileName;
  state.suggestedRules = data.suggestedRules || [];

  renderResponse(data);
  setStatus(`Loaded ${data.rowCount} rows from ${data.fileName}.`);
}

async function applyRules() {
  if (!state.importId) {
    setStatus("No import loaded.");
    return;
  }

  const selectedRules = Array.from(document.querySelectorAll(".rule-checkbox"))
    .filter(x => x.checked)
    .map(x => state.suggestedRules[Number(x.dataset.index)]);

  setStatus("Applying transformations...");
  const res = await fetch(`/api/import/${state.importId}/transform`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ rules: selectedRules })
  });

  const data = await res.json();
  if (!res.ok) {
    setStatus(data.error || "Transformation failed.");
    return;
  }

  renderResponse(data);
  setStatus("Transformations applied.");
}

function renderResponse(data) {
  summarySection.classList.remove("hidden");
  transformSection.classList.remove("hidden");
  previewSection.classList.remove("hidden");

  document.getElementById("summary").innerHTML = `
    <span class="badge">Rows: ${data.rowCount}</span>
    <span class="badge">Columns: ${data.headers.length}</span>
    <span class="badge">ImportId: ${data.importId}</span>
  `;

  renderProfileTable(data.profile || []);
  renderAnomalies(data.anomalies || []);
  renderRules(data.suggestedRules || []);
  renderPreview(data.headers || [], data.previewRows || []);

  state.suggestedRules = data.suggestedRules || [];
}

function renderProfileTable(profile) {
  const table = document.getElementById("profileTable");
  table.innerHTML = "";

  const headers = ["Column", "Type", "Total", "NonEmpty", "NullRate", "Unique", "Min", "Max", "MaxLen", "Samples"];
  table.appendChild(buildHeaderRow(headers));

  profile.forEach(p => {
    const tr = document.createElement("tr");
    const cells = [
      p.column,
      p.inferredType,
      p.totalRows,
      p.nonEmptyRows,
      p.nullRate,
      p.uniqueCount,
      p.min ?? "",
      p.max ?? "",
      p.maxLength,
      (p.samples || []).join(" | ")
    ];
    cells.forEach(c => {
      const td = document.createElement("td");
      td.textContent = String(c);
      tr.appendChild(td);
    });
    table.appendChild(tr);
  });
}

function renderAnomalies(anomalies) {
  const box = document.getElementById("anomalyList");
  if (!anomalies.length) {
    box.innerHTML = "<p>No anomalies detected.</p>";
    return;
  }

  box.innerHTML = anomalies
    .map(a => `<div><strong>${escapeHtml(a.column)}</strong> [${escapeHtml(a.code)}]: ${escapeHtml(a.message)} (count=${a.count})</div>`)
    .join("");
}

function renderRules(rules) {
  const box = document.getElementById("rules");
  if (!rules.length) {
    box.innerHTML = "<p>No suggested rules.</p>";
    return;
  }

  box.innerHTML = rules.map((r, idx) => `
    <label class="rule-item">
      <input class="rule-checkbox" data-index="${idx}" type="checkbox" ${r.enabled ? "checked" : ""} />
      <span><strong>${escapeHtml(r.operation)}</strong> on <code>${escapeHtml(r.column)}</code>${r.parameter ? ` -> ${escapeHtml(r.parameter)}` : ""}</span>
    </label>
  `).join("");
}

function renderPreview(headers, rows) {
  const table = document.getElementById("previewTable");
  table.innerHTML = "";
  table.appendChild(buildHeaderRow(headers));

  rows.forEach(row => {
    const tr = document.createElement("tr");
    headers.forEach(h => {
      const td = document.createElement("td");
      td.textContent = row[h] ?? "";
      tr.appendChild(td);
    });
    table.appendChild(tr);
  });
}

function buildHeaderRow(cells) {
  const tr = document.createElement("tr");
  cells.forEach(h => {
    const th = document.createElement("th");
    th.textContent = h;
    tr.appendChild(th);
  });
  return tr;
}

function download(format) {
  if (!state.importId) {
    setStatus("No import loaded.");
    return;
  }

  window.location.href = `/api/import/${state.importId}/download?format=${format}`;
}

function setStatus(msg) {
  statusEl.textContent = msg;
}

function escapeHtml(value) {
  return String(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}
