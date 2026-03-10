// Constants
const MAX_TEXT_PREVIEW_BYTES = 256 * 1024;

const IMAGE_EXTENSIONS = new Set(["png", "jpg", "jpeg", "gif", "webp", "bmp", "svg", "ico", "avif"]);
const VIDEO_EXTENSIONS = new Set(["mp4", "webm", "mov", "m4v", "ogv"]);
const AUDIO_EXTENSIONS = new Set(["mp3", "wav", "ogg", "oga", "m4a", "aac", "flac"]);
const TEXT_EXTENSIONS = new Set([
  "txt", "log", "md", "csv", "tsv", "yaml", "yml", "toml", "xml", "html", "htm",
  "css", "js", "mjs", "cjs", "ts", "tsx", "jsx", "rs", "go", "py", "java", "c",
  "h", "cpp", "hpp", "sh", "sql", "ini", "conf", "json"
]);

// State
const state = {
  buckets: [],
  selectedBucket: "",
  prefix: "",
  folders: [],
  objects: [],
  totalSize: 0,
  version: "0.1.0",
  clusterEnabled: false,
  clusterInfo: null,
  preview: emptyPreviewState(),
  previewRequestId: 0,
  previewAbortController: null,
};

// DOM Elements
const elements = {
  bucketForm: document.querySelector("#bucketForm"),
  bucketNameInput: document.querySelector("#bucketNameInput"),
  refreshBucketsButton: document.querySelector("#refreshBucketsButton"),
  bucketList: document.querySelector("#bucketList"),
  bucketCount: document.querySelector("#bucketCount"),
  clusterStatusIcon: document.querySelector("#clusterStatusIcon"),
  clusterStatusText: document.querySelector("#clusterStatusText"),
  nodeCount: document.querySelector("#nodeCount"),
  clusterMode: document.querySelector("#clusterMode"),
  quorumInfo: document.querySelector("#quorumInfo"),
  versionLabel: document.querySelector("#versionLabel"),
  statusLine: document.querySelector("#statusLine"),
  currentBucketTitle: document.querySelector("#currentBucketTitle"),
  currentPrefixLabel: document.querySelector("#currentPrefixLabel"),
  deleteBucketButton: document.querySelector("#deleteBucketButton"),
  refreshObjectsButton: document.querySelector("#refreshObjectsButton"),
  upButton: document.querySelector("#upButton"),
  breadcrumbBar: document.querySelector("#breadcrumbBar"),
  folderCount: document.querySelector("#folderCount"),
  objectCount: document.querySelector("#objectCount"),
  totalSize: document.querySelector("#totalSize"),
  uploadForm: document.querySelector("#uploadForm"),
  objectNameInput: document.querySelector("#objectNameInput"),
  fileInput: document.querySelector("#fileInput"),
  dropZone: document.querySelector("#dropZone"),
  uploadProgressBar: document.querySelector("#uploadProgressBar"),
  uploadProgressLabel: document.querySelector("#uploadProgressLabel"),
  uploadButton: document.querySelector("#uploadButton"),
  objectTableBody: document.querySelector("#objectTableBody"),
  emptyState: document.querySelector("#emptyState"),
  previewPanel: document.querySelector("#previewPanel"),
  toastRack: document.querySelector("#toastRack"),
  settingsButton: document.querySelector("#settingsButton"),
  settingsModal: document.querySelector("#settingsModal"),
  clusterTopology: document.querySelector("#clusterTopology"),
  serverVersion: document.querySelector("#serverVersion"),
  serverDataDir: document.querySelector("#serverDataDir"),
  serverClusterMode: document.querySelector("#serverClusterMode"),
  serverNodeId: document.querySelector("#serverNodeId"),
};

// Initialize
document.addEventListener("DOMContentLoaded", () => {
  bindEvents();
  refreshHealth();
  loadBuckets();
  loadClusterInfo();
});

function bindEvents() {
  // Bucket form
  elements.bucketForm.addEventListener("submit", handleBucketCreate);
  elements.refreshBucketsButton.addEventListener("click", () => loadBuckets());
  
  // Object operations
  elements.refreshObjectsButton.addEventListener("click", () => loadObjects());
  elements.deleteBucketButton.addEventListener("click", handleBucketDelete);
  elements.upButton.addEventListener("click", handleNavigateUp);
  
  // Upload
  elements.uploadForm.addEventListener("submit", handleUpload);
  elements.fileInput.addEventListener("change", syncObjectNameFromFile);
  
  // Table actions
  elements.objectTableBody.addEventListener("click", handleTableAction);
  elements.previewPanel.addEventListener("click", handlePreviewAction);
  
  // Drag and drop
  ["dragenter", "dragover"].forEach(eventName => {
    elements.dropZone.addEventListener(eventName, (e) => {
      e.preventDefault();
      elements.dropZone.classList.add("is-dragging");
    });
  });
  
  ["dragleave", "drop"].forEach(eventName => {
    elements.dropZone.addEventListener(eventName, (e) => {
      e.preventDefault();
      elements.dropZone.classList.remove("is-dragging");
    });
  });
  
  elements.dropZone.addEventListener("drop", handleFileDrop);
  elements.dropZone.addEventListener("click", () => elements.fileInput.click());
  
  // Settings modal
  elements.settingsButton?.addEventListener("click", openSettings);
  document.querySelector(".modal-backdrop")?.addEventListener("click", closeSettings);
  document.querySelector(".modal-close")?.addEventListener("click", closeSettings);
}

async function refreshHealth() {
  try {
    const health = await fetchJson("/health");
    state.version = health.version || state.version;
    state.clusterEnabled = health.cluster_mode || false;
    
    elements.versionLabel.textContent = `v${state.version}`;
    elements.statusLine.textContent = health.data_dir 
      ? health.data_dir.split('/').pop() 
      : "Ready";
    
    // Update server info in settings
    elements.serverVersion.textContent = state.version;
    elements.serverDataDir.textContent = health.data_dir || "-";
    elements.serverClusterMode.textContent = health.cluster_mode ? "Cluster" : "Single Node";
    elements.serverNodeId.textContent = health.node_id || "N/A";
    
  } catch (error) {
    elements.statusLine.textContent = "Error connecting";
  }
}

async function loadClusterInfo() {
  try {
    const response = await fetchJson("/cluster/health");
    
    if (response.enabled) {
      state.clusterInfo = response;
      elements.clusterStatusIcon.classList.add("healthy");
      elements.clusterStatusIcon.textContent = "●";
      elements.clusterStatusText.textContent = response.healthy ? "Cluster Healthy" : "Degraded";
      elements.nodeCount.textContent = `${response.healthy_nodes}/${response.total_nodes}`;
      elements.clusterMode.textContent = "Distributed";
      elements.quorumInfo.textContent = `W:${response.write_quorum} R:${response.read_quorum}`;
      elements.serverNodeId.textContent = response.node_id || "N/A";
    } else {
      elements.clusterStatusIcon.textContent = "●";
      elements.clusterStatusText.textContent = "Single Node";
      elements.nodeCount.textContent = "1";
      elements.clusterMode.textContent = "Standalone";
      elements.quorumInfo.textContent = "N/A";
    }
  } catch (error) {
    elements.clusterStatusIcon.textContent = "●";
    elements.clusterStatusText.textContent = "Single Node";
    elements.nodeCount.textContent = "1";
    elements.clusterMode.textContent = "Standalone";
    elements.quorumInfo.textContent = "N/A";
  }
}

async function loadBuckets(preferredBucket) {
  try {
    const payload = await fetchJson("/buckets");
    state.buckets = payload.buckets || [];
    
    console.log("Buckets loaded:", state.buckets); // Debug
    
    const bucketText = state.buckets.length === 1 
      ? "1 Bucket" 
      : `${state.buckets.length} Buckets`;
    elements.bucketCount.textContent = bucketText;
    
    if (state.buckets.length === 0) {
      state.selectedBucket = "";
      state.prefix = "";
      state.folders = [];
      state.objects = [];
      clearPreview(false);
      renderBucketList();
      renderWorkspace();
      return;
    }
    
    const nextBucket = preferredBucket || state.selectedBucket || state.buckets[0]?.name || "";
    const hasNextBucket = nextBucket && state.buckets.some(b => b.name === nextBucket);
    
    if (hasNextBucket) {
      state.selectedBucket = nextBucket;
      if (state.prefix) state.prefix = "";
      clearPreview(false);
      renderBucketList();
      renderWorkspace();
      await loadObjects();
    } else {
      state.selectedBucket = "";
      state.prefix = "";
      state.folders = [];
      state.objects = [];
      clearPreview(false);
      renderBucketList();
      renderWorkspace();
    }
  } catch (error) {
    notify(error.message, "error");
  }
}

async function loadObjects() {
  state.folders = [];
  state.objects = [];
  state.totalSize = 0;
  renderWorkspace();
  
  if (!state.selectedBucket) return;
  
  try {
    const query = new URLSearchParams();
    if (state.prefix) query.set("prefix", state.prefix);
    query.set("delimiter", "/");
    
    const payload = await fetchJson(
      `/objects/${encodeURIComponent(state.selectedBucket)}?${query.toString()}`
    );
    
    state.folders = payload.common_prefixes || [];
    state.objects = payload.objects || [];
    state.totalSize = state.objects.reduce((sum, obj) => sum + (obj.size || 0), 0);
    
    syncPreviewSelection();
    renderWorkspace();
    
    if (state.preview.object) {
      void loadObjectPreview(state.preview.object, { renderSelection: false });
    }
  } catch (error) {
    notify(error.message, "error");
  }
}

async function handleBucketCreate(event) {
  event.preventDefault();
  const name = elements.bucketNameInput.value.trim().toLowerCase();
  
  if (!name || name.length < 3) {
    notify("Bucket name must be at least 3 characters", "error");
    return;
  }
  
  if (!/^[a-z0-9][a-z0-9.-]*[a-z0-9]$/.test(name)) {
    notify("Bucket name must start and end with lowercase letters or numbers", "error");
    return;
  }
  
  try {
    await fetchJson(`/buckets/${encodeURIComponent(name)}`, { method: "PUT" });
    elements.bucketNameInput.value = "";
    notify(`Bucket "${name}" created successfully`, "success");
    await loadBuckets(name);
  } catch (error) {
    notify(error.message, "error");
  }
}

async function handleBucketDelete() {
  if (!state.selectedBucket) {
    notify("Select a bucket first", "error");
    return;
  }
  
  if (!window.confirm(`Delete bucket "${state.selectedBucket}"? This cannot be undone.`)) {
    return;
  }
  
  try {
    await fetchJson(`/buckets/${encodeURIComponent(state.selectedBucket)}`, {
      method: "DELETE",
    });
    notify(`Bucket "${state.selectedBucket}" deleted`, "success");
    state.selectedBucket = "";
    state.prefix = "";
    clearPreview(false);
    await loadBuckets();
  } catch (error) {
    notify(error.message, "error");
  }
}

function handleNavigateUp() {
  if (!state.selectedBucket || !state.prefix) return;
  clearPreview(false);
  state.prefix = parentPrefix(state.prefix);
  loadObjects();
}

function handleFileDrop(event) {
  const files = event.dataTransfer?.files;
  if (!files || files.length === 0) return;
  
  elements.fileInput.files = files;
  syncObjectNameFromFile();
  notify(`Selected ${files.length} file(s)`, "success");
}

async function handleUpload(event) {
  event.preventDefault();
  
  if (!state.selectedBucket) {
    notify("Select a bucket first", "error");
    return;
  }
  
  const file = elements.fileInput.files?.[0];
  if (!file) {
    notify("Select a file to upload", "error");
    return;
  }
  
  const requestedName = elements.objectNameInput.value.trim();
  const key = normalizeObjectKey(requestedName || file.name);
  
  if (!key) {
    notify("Invalid object key", "error");
    return;
  }
  
  try {
    setUploadProgress(0, "Uploading...");
    elements.uploadButton.disabled = true;
    
    await uploadObject(state.selectedBucket, key, file);
    
    elements.fileInput.value = "";
    elements.objectNameInput.value = "";
    setUploadProgress(100, "Complete");
    notify(`Uploaded "${key}"`, "success");
    await loadObjects();
    
  } catch (error) {
    setUploadProgress(0, "Failed");
    notify(error.message, "error");
  } finally {
    elements.uploadButton.disabled = false;
  }
}

function handleTableAction(event) {
  const button = event.target.closest("button");
  if (!button) return;
  
  const action = button.dataset.action;
  
  switch (action) {
    case "open-prefix":
      clearPreview(false);
      state.prefix = button.dataset.prefix || "";
      loadObjects();
      break;
    case "preview-object":
      previewObject(button.dataset.key);
      break;
    case "download-object":
      downloadObject(button.dataset.key);
      break;
    case "delete-object":
      deleteObject(button.dataset.key);
      break;
  }
}

function handlePreviewAction(event) {
  const button = event.target.closest("button");
  if (!button) return;
  
  const action = button.dataset.action;
  
  switch (action) {
    case "close-preview":
      clearPreview();
      break;
    case "refresh-preview":
      if (state.preview.object) {
        void loadObjectPreview(state.preview.object);
      }
      break;
    case "download-object":
      downloadObject(button.dataset.key);
      break;
  }
}

async function deleteObject(key) {
  if (!window.confirm(`Delete object "${key}"?`)) return;
  
  try {
    await fetchJson(objectUrl(state.selectedBucket, key), { method: "DELETE" });
    if (state.preview.key === key) clearPreview(false);
    notify(`Deleted "${key}"`, "success");
    await loadObjects();
  } catch (error) {
    notify(error.message, "error");
  }
}

// Render functions
function renderBucketList() {
  if (state.buckets.length === 0) {
    elements.bucketList.innerHTML = `
      <div class="empty-state" style="padding: 1.5rem;">
        <p class="empty-title" style="font-size: 0.875rem;">No buckets</p>
        <p class="empty-copy" style="font-size: 0.8125rem;">Create a bucket to get started</p>
      </div>
    `;
    return;
  }
  
  elements.bucketList.innerHTML = state.buckets.map(bucket => {
    const isActive = bucket.name === state.selectedBucket;
    return `
      <button class="bucket-button ${isActive ? 'is-active' : ''}" data-bucket="${escapeHtml(bucket.name)}" type="button">
        <span class="bucket-icon">
          <svg viewBox="0 0 24 24" width="18" height="18" fill="none" stroke="currentColor" stroke-width="2">
            <path d="M22 19a2 2 0 0 1-2 2H4a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h5l2 3h9a2 2 0 0 1 2 2z"/>
          </svg>
        </span>
        <span class="bucket-info">
          <span class="bucket-name">${escapeHtml(bucket.name)}</span>
          <span class="bucket-date">${formatTimestamp(bucket.created_at)}</span>
        </span>
      </button>
    `;
  }).join("");
  
  elements.bucketList.querySelectorAll("[data-bucket]").forEach(btn => {
    btn.addEventListener("click", async () => {
      clearPreview(false);
      state.selectedBucket = btn.dataset.bucket;
      state.prefix = "";
      renderBucketList();
      await loadObjects();
    });
  });
}

function renderWorkspace() {
  elements.currentBucketTitle.textContent = state.selectedBucket || "No Bucket Selected";
  elements.currentPrefixLabel.textContent = state.selectedBucket
    ? `Browsing ${state.prefix || "root"}`
    : "Create or select a bucket to start";
  
  elements.deleteBucketButton.disabled = !state.selectedBucket;
  elements.upButton.disabled = !state.selectedBucket || !state.prefix;
  elements.refreshObjectsButton.disabled = !state.selectedBucket;
  elements.folderCount.textContent = String(state.folders.length);
  elements.objectCount.textContent = String(state.objects.length);
  elements.totalSize.textContent = formatBytes(state.totalSize);
  
  renderBreadcrumbs();
  renderObjectTable();
  renderPreviewPanel();
}

function renderBreadcrumbs() {
  const parts = state.prefix.split("/").filter(Boolean);
  const crumbs = [];
  
  crumbs.push(`<button class="${!state.prefix ? 'crumb-active' : 'crumb'}" data-prefix="">root</button>`);
  
  let built = "";
  for (const part of parts) {
    built += `${part}/`;
    const isActive = built === state.prefix;
    crumbs.push(`<button class="${isActive ? 'crumb-active' : 'crumb'}" data-prefix="${escapeHtml(built)}">${escapeHtml(part)}</button>`);
  }
  
  elements.breadcrumbBar.innerHTML = crumbs.join("");
  
  elements.breadcrumbBar.querySelectorAll("[data-prefix]").forEach(btn => {
    btn.addEventListener("click", () => {
      clearPreview(false);
      state.prefix = btn.dataset.prefix || "";
      loadObjects();
    });
  });
}

function renderObjectTable() {
  const rows = [];
  
  // Folders
  for (const prefix of state.folders) {
    const folderName = trimTrailingSlash(prefix.slice(state.prefix.length)) || prefix;
    rows.push(`
      <tr>
        <td>
          <div class="row-name">
            <button class="prefix-link" data-action="open-prefix" data-prefix="${escapeHtml(prefix)}">
              <svg viewBox="0 0 24 24" width="16" height="16" fill="none" stroke="currentColor" stroke-width="2" style="margin-right: 0.5rem; vertical-align: middle;">
                <path d="M22 19a2 2 0 0 1-2 2H4a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h5l2 3h9a2 2 0 0 1 2 2z"/>
              </svg>
              <strong>${escapeHtml(folderName)}</strong>
            </button>
          </div>
        </td>
        <td><span class="object-kind is-prefix">folder</span></td>
        <td>-</td>
        <td>-</td>
        <td class="row-actions">
          <button class="table-button" data-action="open-prefix" data-prefix="${escapeHtml(prefix)}">Open</button>
        </td>
      </tr>
    `);
  }
  
  // Objects
  for (const obj of state.objects) {
    const name = obj.key.startsWith(state.prefix)
      ? obj.key.slice(state.prefix.length)
      : obj.key;
    const selectedClass = state.preview.key === obj.key ? " is-selected" : "";
    
    rows.push(`
      <tr class="object-row${selectedClass}">
        <td>
          <div class="row-name">
            <button class="object-link" data-action="preview-object" data-key="${escapeHtml(obj.key)}">
              <strong>${escapeHtml(name)}</strong>
            </button>
            <span class="row-meta">${escapeHtml(obj.key)}</span>
          </div>
        </td>
        <td><span class="object-kind">${escapeHtml(formatObjectType(obj))}</span></td>
        <td>${escapeHtml(formatBytes(obj.size))}</td>
        <td>${escapeHtml(formatTimestamp(obj.created_at))}</td>
        <td class="row-actions">
          <button class="table-button" data-action="preview-object" data-key="${escapeHtml(obj.key)}">View</button>
          <button class="table-button" data-action="download-object" data-key="${escapeHtml(obj.key)}">Download</button>
          <button class="table-button is-danger" data-action="delete-object" data-key="${escapeHtml(obj.key)}">Delete</button>
        </td>
      </tr>
    `);
  }
  
  elements.objectTableBody.innerHTML = rows.join("");
  elements.emptyState.classList.toggle("is-hidden", !state.selectedBucket || rows.length > 0);
}

function renderPreviewPanel() {
  if (!state.selectedBucket || !state.preview.object) {
    elements.previewPanel.innerHTML = `
      <div class="preview-empty">
        <div class="preview-empty-icon">
          <svg viewBox="0 0 24 24" width="48" height="48" fill="none" stroke="currentColor" stroke-width="1">
            <rect x="3" y="3" width="18" height="18" rx="2" ry="2"/>
            <circle cx="8.5" cy="8.5" r="1.5"/>
            <polyline points="21 15 16 10 5 21"/>
          </svg>
        </div>
        <p class="empty-title">Preview Panel</p>
        <p class="empty-copy">Select an object to view details, preview images, or download.</p>
      </div>
    `;
    return;
  }
  
  const { object } = state.preview;
  const name = object.key.startsWith(state.prefix)
    ? object.key.slice(state.prefix.length)
    : object.key;
  const contentType = object.content_type || "application/octet-stream";
  
  elements.previewPanel.innerHTML = `
    <div class="preview-card">
      <div class="preview-header">
        <div>
          <p class="panel-kicker">Preview</p>
          <h3>${escapeHtml(name)}</h3>
          <p class="preview-path">${escapeHtml(object.key)}</p>
        </div>
        <div class="preview-actions">
          <button class="table-button" data-action="refresh-preview">Refresh</button>
          <button class="table-button" data-action="download-object" data-key="${escapeHtml(object.key)}">Download</button>
          <button class="table-button" data-action="close-preview">Close</button>
        </div>
      </div>
      
      <div class="preview-chips">
        <span class="preview-chip">${escapeHtml(formatPreviewModeLabel(state.preview.mode))}</span>
        <span class="preview-chip">${escapeHtml(contentType)}</span>
        <span class="preview-chip">${escapeHtml(formatBytes(object.size))}</span>
      </div>
      
      <div class="preview-stage">
        ${previewStageMarkup(name)}
      </div>
      
      <dl class="preview-metadata">
        <div class="preview-meta-item">
          <dt>Bucket</dt>
          <dd>${escapeHtml(object.bucket)}</dd>
        </div>
        <div class="preview-meta-item">
          <dt>ETag</dt>
          <dd class="preview-code">${escapeHtml(object.etag)}</dd>
        </div>
        <div class="preview-meta-item">
          <dt>Size</dt>
          <dd>${escapeHtml(formatBytes(object.size))}</dd>
        </div>
        <div class="preview-meta-item">
          <dt>Modified</dt>
          <dd>${escapeHtml(formatTimestamp(object.created_at))}</dd>
        </div>
      </dl>
    </div>
  `;
  
  if ((state.preview.mode === "text" || state.preview.mode === "json") && state.preview.status === "ready") {
    const textNode = elements.previewPanel.querySelector("#previewTextContent");
    if (textNode) textNode.textContent = state.preview.text;
  }
}

function previewStageMarkup(name) {
  if (state.preview.status === "loading") {
    return `<div class="preview-state"><p class="preview-state-title">Loading...</p></div>`;
  }
  
  if (state.preview.status === "error") {
    return `<div class="preview-state is-error"><p class="preview-state-title">Error</p><p class="empty-copy">${escapeHtml(state.preview.error)}</p></div>`;
  }
  
  if (state.preview.status === "unsupported") {
    return `<div class="preview-state"><p class="preview-state-title">Preview not available</p><p class="empty-copy">Download to view this file</p></div>`;
  }
  
  if (state.preview.mode === "image") {
    return `<img class="preview-image" src="${escapeHtml(state.preview.url)}" alt="${escapeHtml(name)}">`;
  }
  
  if (state.preview.mode === "video") {
    return `<video class="preview-media" controls src="${escapeHtml(state.preview.url)}"></video>`;
  }
  
  if (state.preview.mode === "audio") {
    return `<audio controls src="${escapeHtml(state.preview.url)}" style="width: 100%;"></audio>`;
  }
  
  if (state.preview.mode === "pdf") {
    return `<iframe class="preview-frame" src="${escapeHtml(state.preview.url)}"></iframe>`;
  }
  
  if (state.preview.mode === "text" || state.preview.mode === "json") {
    const note = state.preview.truncated 
      ? `Showing first ${formatBytes(MAX_TEXT_PREVIEW_BYTES)}` 
      : "Full content";
    return `
      <div class="preview-text-shell">
        <p class="preview-text-note">${note}</p>
        <pre id="previewTextContent" class="preview-text"></pre>
      </div>
    `;
  }
  
  return `<div class="preview-state"><p class="preview-state-title">Preview not available</p></div>`;
}

// Settings Modal
function openSettings() {
  elements.settingsModal.classList.remove("hidden");
  loadClusterTopology();
}

function closeSettings() {
  elements.settingsModal.classList.add("hidden");
}

async function loadClusterTopology() {
  try {
    const response = await fetchJson("/cluster/topology");
    
    if (response.enabled && response.nodes && response.nodes.length > 0) {
      elements.clusterTopology.innerHTML = response.nodes.map(node => `
        <div class="topology-item">
          <span class="topology-status ${node.is_healthy ? '' : 'offline'}"></span>
          <div class="topology-info">
            <span class="topology-name">${escapeHtml(node.id)}</span>
            <span class="topology-address">${escapeHtml(node.host)}:${node.port}</span>
          </div>
          <span class="preview-chip">${escapeHtml(node.role)}</span>
        </div>
      `).join("");
    } else {
      elements.clusterTopology.innerHTML = `
        <div class="empty-state" style="padding: 1.5rem;">
          <p class="empty-copy">No cluster topology available</p>
        </div>
      `;
    }
  } catch (error) {
    elements.clusterTopology.innerHTML = `
      <div class="empty-state" style="padding: 1.5rem;">
        <p class="empty-copy">Cluster information unavailable</p>
      </div>
    `;
  }
}

// Object operations
function syncObjectNameFromFile() {
  const file = elements.fileInput.files?.[0];
  if (!file) return;
  
  if (!elements.objectNameInput.value.trim()) {
    elements.objectNameInput.value = file.name;
  }
  setUploadProgress(0, file.name);
}

function uploadObject(bucket, key, file) {
  return new Promise((resolve, reject) => {
    const xhr = new XMLHttpRequest();
    xhr.open("PUT", objectUrl(bucket, key));
    xhr.setRequestHeader("Content-Type", file.type || "application/octet-stream");
    
    xhr.upload.addEventListener("progress", (e) => {
      if (e.lengthComputable) {
        const percent = Math.round((e.loaded / e.total) * 100);
        setUploadProgress(percent, `${percent}%`);
      }
    });
    
    xhr.addEventListener("load", () => {
      if (xhr.status >= 200 && xhr.status < 300) {
        resolve(xhr.response);
      } else {
        reject(parseXhrError(xhr));
      }
    });
    
    xhr.addEventListener("error", () => reject(new Error("Upload failed")));
    xhr.send(file);
  });
}

async function fetchJson(url, options = {}) {
  const response = await fetch(url, options);
  let payload = null;
  try {
    payload = await response.json();
  } catch (_) {}
  
  if (!response.ok) {
    const msg = payload?.message || payload?.error || `Request failed (${response.status})`;
    throw new Error(msg);
  }
  return payload;
}

function previewObject(key) {
  const object = state.objects.find(o => o.key === key);
  if (!object) {
    notify("Object not found", "error");
    return;
  }
  void loadObjectPreview(object);
}

async function loadObjectPreview(object, options = {}) {
  const { renderSelection = true } = options;
  const mode = detectPreviewMode(object);
  const requestId = ++state.previewRequestId;
  
  cancelPreviewRequest();
  
  state.preview = {
    key: object.key,
    object,
    mode,
    status: mode === "unsupported" ? "unsupported" : "loading",
    text: "",
    truncated: false,
    error: "",
    url: objectUrl(state.selectedBucket, object.key),
  };
  
  if (renderSelection) renderWorkspace();
  else renderPreviewPanel();
  
  if (mode === "unsupported") {
    renderPreviewPanel();
    return;
  }
  
  if (["image", "video", "audio", "pdf"].includes(mode)) {
    state.preview.status = "ready";
    renderPreviewPanel();
    return;
  }
  
  if (object.size === 0) {
    state.preview.status = "ready";
    renderPreviewPanel();
    return;
  }
  
  const controller = new AbortController();
  state.previewAbortController = controller;
  
  try {
    const response = await fetch(state.preview.url, {
      headers: { Range: `bytes=0-${MAX_TEXT_PREVIEW_BYTES - 1}` },
      signal: controller.signal,
    });
    
    if (!response.ok && response.status !== 206) {
      throw new Error(`HTTP ${response.status}`);
    }
    
    if (requestId !== state.previewRequestId || state.preview.key !== object.key) return;
    
    const buffer = await response.arrayBuffer();
    let text = new TextDecoder("utf-8").decode(buffer);
    const truncated = object.size > buffer.byteLength || response.status === 206;
    
    if (mode === "json" && !truncated) {
      try { text = JSON.stringify(JSON.parse(text), null, 2); } catch (_) {}
    }
    
    state.preview.status = "ready";
    state.preview.text = text;
    state.preview.truncated = truncated;
    state.previewAbortController = null;
    renderPreviewPanel();
    
  } catch (error) {
    if (error.name === "AbortError") return;
    if (requestId !== state.previewRequestId || state.preview.key !== object.key) return;
    
    state.preview.status = "error";
    state.preview.error = error.message;
    state.previewAbortController = null;
    renderPreviewPanel();
  }
}

function syncPreviewSelection() {
  if (!state.preview.key) return;
  
  const selectedObject = state.objects.find(o => o.key === state.preview.key);
  if (!selectedObject) {
    clearPreview(false);
    return;
  }
  
  state.preview.object = selectedObject;
  state.preview.url = objectUrl(state.selectedBucket, selectedObject.key);
}

function downloadObject(key) {
  window.open(objectUrl(state.selectedBucket, key), "_blank");
}

function clearPreview(render = true) {
  cancelPreviewRequest();
  state.previewRequestId++;
  state.preview = emptyPreviewState();
  if (render) renderWorkspace();
}

function cancelPreviewRequest() {
  if (state.previewAbortController) {
    state.previewAbortController.abort();
    state.previewAbortController = null;
  }
}

function emptyPreviewState() {
  return {
    key: "",
    object: null,
    mode: "empty",
    status: "idle",
    text: "",
    truncated: false,
    error: "",
    url: "",
  };
}

// Helpers
function detectPreviewMode(object) {
  const contentType = normalizeContentType(object.content_type);
  const ext = objectExtension(object.key);
  
  if (contentType.startsWith("image/") || IMAGE_EXTENSIONS.has(ext)) return "image";
  if (contentType.startsWith("video/") || VIDEO_EXTENSIONS.has(ext)) return "video";
  if (contentType.startsWith("audio/") || AUDIO_EXTENSIONS.has(ext)) return "audio";
  if (contentType === "application/pdf" || ext === "pdf") return "pdf";
  if (contentType.includes("json") || ext === "json") return "json";
  if (isTextContentType(contentType) || TEXT_EXTENSIONS.has(ext)) return "text";
  return "unsupported";
}

function formatObjectType(object) {
  const contentType = object.content_type;
  if (contentType) return contentType.split(";")[0].trim() || "binary";
  const mode = detectPreviewMode(object);
  return mode === "unsupported" ? "binary" : `${mode} file`;
}

function formatPreviewModeLabel(mode) {
  const labels = { image: "Image", video: "Video", audio: "Audio", pdf: "PDF", json: "JSON", text: "Text" };
  return labels[mode] || "Binary";
}

function normalizeContentType(ct) {
  return String(ct || "").split(";")[0].trim().toLowerCase();
}

function isTextContentType(ct) {
  return ct.startsWith("text/") || ["xml", "yaml", "toml", "javascript", "typescript", "x-sh", "sql"].some(e => ct.includes(e));
}

function objectExtension(key) {
  const name = key.split("/").pop() || "";
  const idx = name.lastIndexOf(".");
  return idx === -1 ? "" : name.slice(idx + 1).toLowerCase();
}

function objectUrl(bucket, key) {
  const encodedKey = key.split("/").map(s => encodeURIComponent(s)).join("/");
  return `/objects/${encodeURIComponent(bucket)}/${encodedKey}`;
}

function normalizeObjectKey(name) {
  const clean = name.trim().replace(/^\/+/, "");
  return clean ? `${state.prefix}${clean}` : "";
}

function parentPrefix(prefix) {
  const parts = prefix.split("/").filter(Boolean);
  parts.pop();
  return parts.length ? `${parts.join("/")}/` : "";
}

function trimTrailingSlash(v) {
  return v.endsWith("/") ? v.slice(0, -1) : v;
}

function formatBytes(size) {
  if (!size || size === 0) return "0 B";
  const units = ["B", "KB", "MB", "GB", "TB"];
  let value = Number(size);
  let unitIndex = 0;
  while (value >= 1024 && unitIndex < units.length - 1) {
    value /= 1024;
    unitIndex++;
  }
  return `${value.toFixed(unitIndex === 0 ? 0 : 1)} ${units[unitIndex]}`;
}

function formatTimestamp(ts) {
  if (!ts) return "-";
  const date = new Date(ts * 1000);
  return date.toLocaleDateString() + " " + date.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" });
}

function setUploadProgress(percent, label) {
  elements.uploadProgressBar.style.width = `${Math.max(0, Math.min(percent, 100))}%`;
  elements.uploadProgressLabel.textContent = label;
}

function parseXhrError(xhr) {
  try {
    const payload = JSON.parse(xhr.responseText);
    return new Error(payload.message || payload.error || `Upload failed (${xhr.status})`);
  } catch (_) {
    return new Error(`Upload failed (${xhr.status})`);
  }
}

function notify(message, tone = "info") {
  const toast = document.createElement("div");
  toast.className = `toast ${tone === "error" ? "is-error" : tone === "success" ? "is-success" : ""}`;
  toast.textContent = message;
  elements.toastRack.appendChild(toast);
  setTimeout(() => toast.remove(), 3500);
}

function escapeHtml(value) {
  if (!value) return "";
  return String(value)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
}
