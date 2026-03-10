let state = {
  importId: null,
  fileName: null,
  suggestedRules: [],
  latestJobId: null,
  pollTimer: null
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

  stopPolling();
  setBusy(true);
  setStatus("Uploading file and queueing profiling job...");

  const res = await fetch("/api/import/upload", {
    method: "POST",
    body: formData
  });

  const data = await res.json();
  if (!res.ok) {
    setBusy(false);
    setStatus(data.error || "Upload failed.");
    return;
  }

  state.importId = data.importId;
  state.fileName = data.fileName;
  state.latestJobId = data.jobId || data.latestJobId || null;

  if (data.rowCount) {
    renderResponse(data);
    setBusy(false);
    setStatus(buildLoadedMessage(data));
    return;
  }

  await waitForImport(data.importId, state.latestJobId, "Profiling");
}

async function applyRules() {
  if (!state.importId) {
    setStatus("No import loaded.");
    return;
  }

  const selectedRules = Array.from(document.querySelectorAll(".rule-checkbox"))
    .filter(x => x.checked)
    .map(x => state.suggestedRules[Number(x.dataset.index)]);

  stopPolling();
  setBusy(true);
  setStatus("Queueing transform job...");

  const res = await fetch(`/api/import/${state.importId}/transform`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ rules: selectedRules })
  });

  const data = await res.json();
  if (!res.ok) {
    setBusy(false);
    setStatus(data.error || "Transformation failed.");
    return;
  }

  state.latestJobId = data.jobId || null;
  await waitForImport(state.importId, state.latestJobId, "Transform");
}

async function waitForImport(importId, jobId, label) {
  const startedAt = Date.now();

  const poll = async () => {
    try {
      if (jobId) {
        const job = await fetchJson(`/api/job/${jobId}`);
        if (job.status === "failed") {
          stopPolling();
          setBusy(false);
          setStatus(`${label} failed: ${job.error || "Unknown error"}`);
          return;
        }

        setStatus(`${label} job ${job.status}...`);
      }

      const result = await fetchJson(`/api/import/${importId}`);
      if (result.rowCount) {
        stopPolling();
        renderResponse(result);
        setBusy(false);
        setStatus(buildLoadedMessage(result, Date.now() - startedAt));
        return;
      }
    } catch (error) {
      stopPolling();
      setBusy(false);
      setStatus(error.message || `${label} failed.`);
      return;
    }

    state.pollTimer = window.setTimeout(poll, 1000);
  };

  await poll();
}

function renderResponse(data) {
  summarySection.classList.remove("hidden");
  transformSection.classList.remove("hidden");
  previewSection.classList.remove("hidden");

  document.getElementById("summary").innerHTML = `
    <span class="badge">Rows: ${data.rowCount}</span>
    <span class="badge">Columns: ${data.headers.length}</span>
    <span class="badge">ImportId: ${data.importId}</span>
    <span class="badge">Rows/s: ${Math.round(data.rowsPerSecond || 0)}</span>
    <span class="badge">Exec ms: ${data.executionMs || 0}</span>
  `;

  renderProfileTable(data.profile || []);
  renderAnomalies(data.anomalies || []);
  renderRules(data.suggestedRules || []);
  renderPreview(data.headers || [], data.previewRows || []);

  state.importId = data.importId;
  state.fileName = data.fileName;
  state.suggestedRules = data.suggestedRules || [];
  state.latestJobId = data.latestJobId || null;
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

function setBusy(isBusy) {
  uploadBtn.disabled = isBusy;
  applyBtn.disabled = isBusy;
}

function stopPolling() {
  if (state.pollTimer) {
    window.clearTimeout(state.pollTimer);
    state.pollTimer = null;
  }
}

async function fetchJson(url) {
  const res = await fetch(url);
  const data = await res.json();
  if (!res.ok) {
    throw new Error(data.error || "Request failed.");
  }
  return data;
}

function buildLoadedMessage(data, elapsedMs) {
  const dedupe = data.deduplicated ? " Reused existing import by file hash." : "";
  const elapsed = elapsedMs ? ` Finished in ~${elapsedMs} ms.` : "";
  return `Loaded ${data.rowCount} rows from ${data.fileName}.${dedupe}${elapsed}`;
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
