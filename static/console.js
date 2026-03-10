const MAX_TEXT_PREVIEW_BYTES = 256 * 1024;

const IMAGE_EXTENSIONS = new Set(["png", "jpg", "jpeg", "gif", "webp", "bmp", "svg", "ico", "avif"]);
const VIDEO_EXTENSIONS = new Set(["mp4", "webm", "mov", "m4v", "ogv"]);
const AUDIO_EXTENSIONS = new Set(["mp3", "wav", "ogg", "oga", "m4a", "aac", "flac"]);
const TEXT_EXTENSIONS = new Set([
  "txt",
  "log",
  "md",
  "csv",
  "tsv",
  "yaml",
  "yml",
  "toml",
  "xml",
  "html",
  "htm",
  "css",
  "js",
  "mjs",
  "cjs",
  "ts",
  "tsx",
  "jsx",
  "rs",
  "go",
  "py",
  "java",
  "c",
  "h",
  "cpp",
  "hpp",
  "sh",
  "sql",
  "ini",
  "conf",
]);

const state = {
  buckets: [],
  selectedBucket: "",
  prefix: "",
  folders: [],
  objects: [],
  version: "-",
  preview: emptyPreviewState(),
  previewRequestId: 0,
  previewAbortController: null,
};

const elements = {
  bucketForm: document.querySelector("#bucketForm"),
  bucketNameInput: document.querySelector("#bucketNameInput"),
  refreshBucketsButton: document.querySelector("#refreshBucketsButton"),
  bucketList: document.querySelector("#bucketList"),
  bucketCount: document.querySelector("#bucketCount"),
  healthChip: document.querySelector("#healthChip"),
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
  prefixDepth: document.querySelector("#prefixDepth"),
  uploadForm: document.querySelector("#uploadForm"),
  objectNameInput: document.querySelector("#objectNameInput"),
  fileInput: document.querySelector("#fileInput"),
  dropZone: document.querySelector("#dropZone"),
  uploadProgressBar: document.querySelector("#uploadProgressBar"),
  uploadProgressLabel: document.querySelector("#uploadProgressLabel"),
  objectTableBody: document.querySelector("#objectTableBody"),
  emptyState: document.querySelector("#emptyState"),
  previewPanel: document.querySelector("#previewPanel"),
  toastRack: document.querySelector("#toastRack"),
};

document.addEventListener("DOMContentLoaded", () => {
  bindEvents();
  refreshHealth();
  loadBuckets();
});

function bindEvents() {
  elements.bucketForm.addEventListener("submit", handleBucketCreate);
  elements.refreshBucketsButton.addEventListener("click", () => loadBuckets());
  elements.refreshObjectsButton.addEventListener("click", () => loadObjects());
  elements.deleteBucketButton.addEventListener("click", handleBucketDelete);
  elements.upButton.addEventListener("click", () => {
    if (!state.selectedBucket) {
      return;
    }
    clearPreview(false);
    state.prefix = parentPrefix(state.prefix);
    loadObjects();
  });
  elements.uploadForm.addEventListener("submit", handleUpload);
  elements.fileInput.addEventListener("change", syncObjectNameFromFile);
  elements.objectTableBody.addEventListener("click", handleTableAction);
  elements.previewPanel.addEventListener("click", handlePreviewAction);

  ["dragenter", "dragover"].forEach((eventName) => {
    elements.dropZone.addEventListener(eventName, (event) => {
      event.preventDefault();
      elements.dropZone.classList.add("is-dragging");
    });
  });

  ["dragleave", "drop"].forEach((eventName) => {
    elements.dropZone.addEventListener(eventName, (event) => {
      event.preventDefault();
      elements.dropZone.classList.remove("is-dragging");
    });
  });

  elements.dropZone.addEventListener("drop", (event) => {
    const files = event.dataTransfer?.files;
    if (!files || files.length === 0) {
      return;
    }
    elements.fileInput.files = files;
    syncObjectNameFromFile();
    notify(`Selected ${files[0].name}`, "success");
  });
}

async function refreshHealth() {
  try {
    const health = await fetchJson("/health");
    state.version = health.version || "-";
    elements.healthChip.textContent = "Online";
    elements.healthChip.classList.add("is-ok");
    elements.versionLabel.textContent = `version ${state.version}`;
    elements.statusLine.textContent = health.data_dir
      ? `data dir ${health.data_dir}`
      : "Console ready";
  } catch (error) {
    elements.healthChip.textContent = "Unavailable";
    elements.healthChip.classList.add("is-warn");
    elements.statusLine.textContent = error.message;
  }
}

async function loadBuckets(preferredBucket) {
  try {
    const payload = await fetchJson("/buckets");
    state.buckets = payload.buckets || [];
    elements.bucketCount.textContent = `${state.buckets.length} bucket${state.buckets.length === 1 ? "" : "s"}`;

    const nextBucket =
      preferredBucket ||
      state.selectedBucket ||
      state.buckets[0]?.name ||
      "";
    const hasNextBucket = nextBucket && state.buckets.some((bucket) => bucket.name === nextBucket);
    const bucketChanged = nextBucket !== state.selectedBucket;

    if (hasNextBucket) {
      state.selectedBucket = nextBucket;
      if (bucketChanged) {
        state.prefix = "";
        clearPreview(false);
      }
      renderBucketList();
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
  renderWorkspace();
  if (!state.selectedBucket) {
    return;
  }

  try {
    const prefix = state.prefix || "";
    const query = new URLSearchParams();
    if (prefix) {
      query.set("prefix", prefix);
    }
    query.set("delimiter", "/");

    const payload = await fetchJson(
      `/objects/${encodeURIComponent(state.selectedBucket)}?${query.toString()}`
    );

    state.folders = payload.common_prefixes || [];
    state.objects = payload.objects || [];
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
  const name = elements.bucketNameInput.value.trim();
  if (!name) {
    notify("Bucket name is required", "error");
    return;
  }

  try {
    await fetchJson(`/buckets/${encodeURIComponent(name)}`, { method: "PUT" });
    elements.bucketNameInput.value = "";
    notify(`Bucket ${name} created`, "success");
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

  if (!window.confirm(`Delete bucket ${state.selectedBucket}? It must be empty.`)) {
    return;
  }

  try {
    await fetchJson(`/buckets/${encodeURIComponent(state.selectedBucket)}`, {
      method: "DELETE",
    });
    notify(`Bucket ${state.selectedBucket} deleted`, "success");
    state.selectedBucket = "";
    state.prefix = "";
    clearPreview(false);
    await loadBuckets();
  } catch (error) {
    notify(error.message, "error");
  }
}

async function handleUpload(event) {
  event.preventDefault();

  if (!state.selectedBucket) {
    notify("Select a bucket before uploading", "error");
    return;
  }

  const file = elements.fileInput.files?.[0];
  if (!file) {
    notify("Choose a file to upload", "error");
    return;
  }

  const requestedName = elements.objectNameInput.value.trim();
  const key = normalizeObjectKey(requestedName || file.name);
  if (!key) {
    notify("Object key is invalid", "error");
    return;
  }

  try {
    setUploadProgress(0, "Uploading...");
    await uploadObject(state.selectedBucket, key, file);
    elements.fileInput.value = "";
    elements.objectNameInput.value = "";
    setUploadProgress(100, "Complete");
    notify(`Uploaded ${key}`, "success");
    await loadObjects();
  } catch (error) {
    setUploadProgress(0, "Upload failed");
    notify(error.message, "error");
  }
}

function handleTableAction(event) {
  const button = event.target.closest("button");
  if (!button) {
    return;
  }

  const action = button.dataset.action;
  if (action === "open-prefix") {
    clearPreview(false);
    state.prefix = button.dataset.prefix || "";
    loadObjects();
    return;
  }

  if (action === "preview-object") {
    const key = button.dataset.key;
    if (key) {
      previewObject(key);
    }
    return;
  }

  if (action === "download-object") {
    const key = button.dataset.key;
    if (key) {
      downloadObject(key);
    }
    return;
  }

  if (action === "delete-object") {
    const key = button.dataset.key;
    if (key) {
      deleteObject(key);
    }
  }
}

function handlePreviewAction(event) {
  const button = event.target.closest("button");
  if (!button) {
    return;
  }

  const action = button.dataset.action;
  if (action === "close-preview") {
    clearPreview();
    return;
  }

  if (action === "refresh-preview") {
    if (state.preview.object) {
      void loadObjectPreview(state.preview.object);
    }
    return;
  }

  if (action === "download-object") {
    const key = button.dataset.key;
    if (key) {
      downloadObject(key);
    }
  }
}

async function deleteObject(key) {
  if (!window.confirm(`Delete object ${key}?`)) {
    return;
  }

  try {
    await fetchJson(objectUrl(state.selectedBucket, key), { method: "DELETE" });
    if (state.preview.key === key) {
      clearPreview(false);
    }
    notify(`Deleted ${key}`, "success");
    await loadObjects();
  } catch (error) {
    notify(error.message, "error");
  }
}

function renderBucketList() {
  if (state.buckets.length === 0) {
    elements.bucketList.innerHTML = `
      <div class="empty-state">
        <p class="empty-title">No buckets yet</p>
        <p class="empty-copy">Create one from the form above.</p>
      </div>
    `;
    return;
  }

  elements.bucketList.innerHTML = state.buckets
    .map((bucket) => {
      const activeClass = bucket.name === state.selectedBucket ? " is-active" : "";
      const createdAt = formatTimestamp(bucket.created_at);
      return `
        <button class="bucket-button${activeClass}" type="button" data-bucket="${escapeHtml(bucket.name)}">
          <strong>${escapeHtml(bucket.name)}</strong>
          <span>created ${escapeHtml(createdAt)}</span>
        </button>
      `;
    })
    .join("");

  elements.bucketList
    .querySelectorAll("[data-bucket]")
    .forEach((button) => button.addEventListener("click", async () => {
      clearPreview(false);
      state.selectedBucket = button.dataset.bucket || "";
      state.prefix = "";
      renderBucketList();
      await loadObjects();
    }));
}

function renderWorkspace() {
  elements.currentBucketTitle.textContent = state.selectedBucket || "No bucket selected";
  elements.currentPrefixLabel.textContent = state.selectedBucket
    ? `Browsing ${state.prefix || "root"}`
    : "Create or select a bucket to start browsing objects.";
  elements.deleteBucketButton.disabled = !state.selectedBucket;
  elements.upButton.disabled = !state.selectedBucket || !state.prefix;
  elements.refreshObjectsButton.disabled = !state.selectedBucket;
  elements.folderCount.textContent = String(state.folders.length);
  elements.objectCount.textContent = String(state.objects.length);
  elements.prefixDepth.textContent = state.prefix ? `${state.prefix.split("/").filter(Boolean).length} levels` : "root";

  renderBreadcrumbs();
  renderObjectTable();
  renderPreviewPanel();
}

function renderBreadcrumbs() {
  const parts = state.prefix.split("/").filter(Boolean);
  const crumbs = [];

  crumbs.push(`
    <button class="${state.prefix ? "crumb" : "crumb-active"}" type="button" data-prefix="">
      root
    </button>
  `);

  let built = "";
  for (const part of parts) {
    built += `${part}/`;
    const active = built === state.prefix;
    crumbs.push(`
      <button class="${active ? "crumb-active" : "crumb"}" type="button" data-prefix="${escapeHtml(built)}">
        ${escapeHtml(part)}
      </button>
    `);
  }

  elements.breadcrumbBar.innerHTML = crumbs.join("");
  elements.breadcrumbBar
    .querySelectorAll("[data-prefix]")
    .forEach((button) => button.addEventListener("click", () => {
      clearPreview(false);
      state.prefix = button.dataset.prefix || "";
      loadObjects();
    }));
}

function renderObjectTable() {
  const rows = [];

  for (const prefix of state.folders) {
    const folderName = trimTrailingSlash(prefix.slice(state.prefix.length)) || prefix;
    rows.push(`
      <tr>
        <td>
          <div class="row-name">
            <button class="prefix-link" type="button" data-action="open-prefix" data-prefix="${escapeHtml(prefix)}">
              <strong>${escapeHtml(folderName)}</strong>
            </button>
            <span class="row-meta">${escapeHtml(prefix)}</span>
          </div>
        </td>
        <td><span class="object-kind is-prefix">prefix</span></td>
        <td>-</td>
        <td>-</td>
        <td class="row-actions">
          <button class="table-button" type="button" data-action="open-prefix" data-prefix="${escapeHtml(prefix)}">
            Open
          </button>
        </td>
      </tr>
    `);
  }

  for (const object of state.objects) {
    const name = object.key.startsWith(state.prefix)
      ? object.key.slice(state.prefix.length)
      : object.key;
    const selectedClass = state.preview.key === object.key ? " is-selected" : "";
    rows.push(`
      <tr class="object-row${selectedClass}">
        <td>
          <div class="row-name">
            <button class="object-link" type="button" data-action="preview-object" data-key="${escapeHtml(object.key)}">
              <strong>${escapeHtml(name)}</strong>
            </button>
            <span class="row-meta">${escapeHtml(object.key)}</span>
          </div>
        </td>
        <td><span class="object-kind">${escapeHtml(formatObjectType(object))}</span></td>
        <td>${escapeHtml(formatBytes(object.size))}</td>
        <td>${escapeHtml(formatTimestamp(object.created_at))}</td>
        <td class="row-actions">
          <button class="table-button" type="button" data-action="preview-object" data-key="${escapeHtml(object.key)}">
            Preview
          </button>
          <button class="table-button" type="button" data-action="download-object" data-key="${escapeHtml(object.key)}">
            Download
          </button>
          <button class="table-button is-danger" type="button" data-action="delete-object" data-key="${escapeHtml(object.key)}">
            Delete
          </button>
        </td>
      </tr>
    `);
  }

  elements.objectTableBody.innerHTML = rows.join("");
  const showEmpty = !state.selectedBucket || rows.length === 0;
  elements.emptyState.classList.toggle("is-hidden", !showEmpty);
}

function renderPreviewPanel() {
  if (!state.selectedBucket || !state.preview.object) {
    elements.previewPanel.innerHTML = `
      <div class="preview-empty">
        <p class="empty-title">Preview panel</p>
        <p class="empty-copy">Select an object to inspect metadata and preview images, text, PDF, audio, or video inline.</p>
      </div>
    `;
    return;
  }

  const { object } = state.preview;
  const name = object.key.startsWith(state.prefix)
    ? object.key.slice(state.prefix.length)
    : object.key;
  const previewUrl = escapeHtml(state.preview.url);
  const contentType = object.content_type || "application/octet-stream";
  const previewStage = previewStageMarkup(name);

  elements.previewPanel.innerHTML = `
    <div class="preview-card">
      <div class="preview-header">
        <div>
          <p class="panel-kicker">Preview</p>
          <h3>${escapeHtml(name)}</h3>
          <p class="preview-path">${escapeHtml(object.key)}</p>
        </div>
        <div class="preview-actions">
          <button class="table-button" type="button" data-action="refresh-preview">Refresh</button>
          <button class="table-button" type="button" data-action="download-object" data-key="${escapeHtml(object.key)}">Download</button>
          <button class="table-button" type="button" data-action="close-preview">Close</button>
        </div>
      </div>

      <div class="preview-chips">
        <span class="preview-chip">${escapeHtml(formatPreviewModeLabel(state.preview.mode))}</span>
        <span class="preview-chip">${escapeHtml(contentType)}</span>
        <span class="preview-chip">${escapeHtml(formatBytes(object.size))}</span>
      </div>

      <div class="preview-stage">
        ${previewStage}
      </div>

      <dl class="preview-metadata">
        <div class="preview-meta-item">
          <dt>Bucket</dt>
          <dd>${escapeHtml(object.bucket)}</dd>
        </div>
        <div class="preview-meta-item">
          <dt>Key</dt>
          <dd class="preview-code">${escapeHtml(object.key)}</dd>
        </div>
        <div class="preview-meta-item">
          <dt>ETag</dt>
          <dd class="preview-code">${escapeHtml(object.etag)}</dd>
        </div>
        <div class="preview-meta-item">
          <dt>Updated</dt>
          <dd>${escapeHtml(formatTimestamp(object.created_at))}</dd>
        </div>
        <div class="preview-meta-item">
          <dt>Direct URL</dt>
          <dd class="preview-code">${previewUrl}</dd>
        </div>
      </dl>
    </div>
  `;

  if ((state.preview.mode === "text" || state.preview.mode === "json") && state.preview.status === "ready") {
    const textNode = elements.previewPanel.querySelector("#previewTextContent");
    if (textNode) {
      textNode.textContent = state.preview.text;
    }
  }
}

function previewStageMarkup(name) {
  if (state.preview.status === "loading") {
    return `
      <div class="preview-state">
        <p class="preview-state-title">Loading preview...</p>
        <p class="empty-copy">Fetching a browser-safe preview for ${escapeHtml(name)}.</p>
      </div>
    `;
  }

  if (state.preview.status === "error") {
    return `
      <div class="preview-state is-error">
        <p class="preview-state-title">Preview unavailable</p>
        <p class="empty-copy">${escapeHtml(state.preview.error || "Unknown preview error")}</p>
      </div>
    `;
  }

  if (state.preview.status === "unsupported") {
    return `
      <div class="preview-state">
        <p class="preview-state-title">This object cannot be previewed inline</p>
        <p class="empty-copy">Download it directly or store it with a previewable content type such as text, image, video, audio, JSON, or PDF.</p>
      </div>
    `;
  }

  if (state.preview.mode === "image") {
    return `<img class="preview-image" src="${escapeHtml(state.preview.url)}" alt="${escapeHtml(name)}">`;
  }

  if (state.preview.mode === "video") {
    return `<video class="preview-media" controls preload="metadata" src="${escapeHtml(state.preview.url)}"></video>`;
  }

  if (state.preview.mode === "audio") {
    return `
      <div class="preview-audio-shell">
        <audio class="preview-audio" controls preload="metadata" src="${escapeHtml(state.preview.url)}"></audio>
      </div>
    `;
  }

  if (state.preview.mode === "pdf") {
    return `<iframe class="preview-frame" src="${escapeHtml(state.preview.url)}" title="${escapeHtml(name)}"></iframe>`;
  }

  if (state.preview.mode === "text" || state.preview.mode === "json") {
    const note = state.preview.truncated
      ? `Showing the first ${formatBytes(MAX_TEXT_PREVIEW_BYTES)} for fast preview.`
      : "Showing the full text preview.";
    return `
      <div class="preview-text-shell">
        <p class="preview-text-note">${escapeHtml(note)}</p>
        <pre id="previewTextContent" class="preview-text"></pre>
      </div>
    `;
  }

  return `
    <div class="preview-state">
      <p class="preview-state-title">Preview unavailable</p>
      <p class="empty-copy">This object type is not supported yet.</p>
    </div>
  `;
}

function syncObjectNameFromFile() {
  const file = elements.fileInput.files?.[0];
  if (!file) {
    return;
  }

  if (!elements.objectNameInput.value.trim()) {
    elements.objectNameInput.value = file.name;
  }
  setUploadProgress(0, file.name);
}

function uploadObject(bucket, key, file) {
  return new Promise((resolve, reject) => {
    const xhr = new XMLHttpRequest();
    xhr.open("PUT", objectUrl(bucket, key));
    xhr.responseType = "json";
    xhr.setRequestHeader("Content-Type", file.type || "application/octet-stream");

    xhr.upload.addEventListener("progress", (event) => {
      if (!event.lengthComputable) {
        setUploadProgress(0, "Uploading...");
        return;
      }

      const percent = Math.round((event.loaded / event.total) * 100);
      setUploadProgress(percent, `${percent}%`);
    });

    xhr.addEventListener("load", () => {
      if (xhr.status >= 200 && xhr.status < 300) {
        resolve(xhr.response);
        return;
      }

      reject(parseXhrError(xhr));
    });

    xhr.addEventListener("error", () => {
      reject(new Error("Upload request failed"));
    });

    xhr.send(file);
  });
}

async function fetchJson(url, options) {
  const response = await fetch(url, options);
  let payload = null;

  try {
    payload = await response.json();
  } catch (_error) {
    payload = null;
  }

  if (!response.ok) {
    const message = payload?.message || payload?.error || `Request failed with status ${response.status}`;
    throw new Error(message);
  }

  return payload;
}

function previewObject(key) {
  const object = state.objects.find((entry) => entry.key === key);
  if (!object) {
    notify(`Object ${key} is no longer available`, "error");
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

  if (renderSelection) {
    renderWorkspace();
  } else {
    renderPreviewPanel();
  }

  if (mode === "unsupported") {
    renderPreviewPanel();
    return;
  }

  if (mode === "image" || mode === "video" || mode === "audio" || mode === "pdf") {
    state.preview.status = "ready";
    renderPreviewPanel();
    return;
  }

  if (object.size === 0) {
    state.preview.status = "ready";
    state.preview.text = "";
    renderPreviewPanel();
    return;
  }

  const controller = new AbortController();
  state.previewAbortController = controller;

  try {
    const response = await fetch(state.preview.url, {
      headers: {
        Range: `bytes=0-${MAX_TEXT_PREVIEW_BYTES - 1}`,
      },
      signal: controller.signal,
    });

    if (!response.ok && response.status !== 206) {
      throw new Error(`Preview request failed with status ${response.status}`);
    }

    const buffer = await response.arrayBuffer();
    const bytes = new Uint8Array(buffer);

    if (requestId !== state.previewRequestId || state.preview.key !== object.key) {
      return;
    }

    let text = new TextDecoder("utf-8").decode(bytes);
    const truncated = object.size > bytes.length || response.status === 206;

    if (mode === "json" && !truncated) {
      try {
        text = JSON.stringify(JSON.parse(text), null, 2);
      } catch (_error) {
        // Keep the original payload if it is not valid JSON text.
      }
    }

    state.preview.status = "ready";
    state.preview.text = text;
    state.preview.truncated = truncated;
    state.previewAbortController = null;
    renderPreviewPanel();
  } catch (error) {
    if (error.name === "AbortError") {
      return;
    }

    if (requestId !== state.previewRequestId || state.preview.key !== object.key) {
      return;
    }

    state.preview.status = "error";
    state.preview.error = error.message || "Preview request failed";
    state.previewAbortController = null;
    renderPreviewPanel();
  }
}

function syncPreviewSelection() {
  if (!state.preview.key) {
    return;
  }

  const selectedObject = state.objects.find((object) => object.key === state.preview.key);
  if (!selectedObject) {
    clearPreview(false);
    return;
  }

  state.preview.object = selectedObject;
  state.preview.url = objectUrl(state.selectedBucket, selectedObject.key);
}

function downloadObject(key) {
  window.open(objectUrl(state.selectedBucket, key), "_blank", "noopener");
}

function clearPreview(render = true) {
  cancelPreviewRequest();
  state.previewRequestId += 1;
  state.preview = emptyPreviewState();
  if (render) {
    renderWorkspace();
  }
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

function detectPreviewMode(object) {
  const contentType = normalizeContentType(object.content_type);
  const extension = objectExtension(object.key);

  if (contentType.startsWith("image/") || IMAGE_EXTENSIONS.has(extension)) {
    return "image";
  }

  if (contentType.startsWith("video/") || VIDEO_EXTENSIONS.has(extension)) {
    return "video";
  }

  if (contentType.startsWith("audio/") || AUDIO_EXTENSIONS.has(extension)) {
    return "audio";
  }

  if (contentType === "application/pdf" || extension === "pdf") {
    return "pdf";
  }

  if (contentType.includes("json") || extension === "json") {
    return "json";
  }

  if (isTextContentType(contentType) || TEXT_EXTENSIONS.has(extension)) {
    return "text";
  }

  return "unsupported";
}

function formatObjectType(object) {
  const contentType = object.content_type;
  if (contentType) {
    return contentType;
  }

  const mode = detectPreviewMode(object);
  if (mode === "unsupported") {
    return "binary";
  }

  return `${formatPreviewModeLabel(mode).toLowerCase()} object`;
}

function formatPreviewModeLabel(mode) {
  if (mode === "image") {
    return "Image";
  }
  if (mode === "video") {
    return "Video";
  }
  if (mode === "audio") {
    return "Audio";
  }
  if (mode === "pdf") {
    return "PDF";
  }
  if (mode === "json") {
    return "JSON";
  }
  if (mode === "text") {
    return "Text";
  }
  return "Binary";
}

function normalizeContentType(contentType) {
  return String(contentType || "")
    .split(";")[0]
    .trim()
    .toLowerCase();
}

function isTextContentType(contentType) {
  return contentType.startsWith("text/") ||
    contentType.includes("xml") ||
    contentType.includes("yaml") ||
    contentType.includes("toml") ||
    contentType.includes("javascript") ||
    contentType.includes("typescript") ||
    contentType.includes("x-sh") ||
    contentType.includes("sql");
}

function objectExtension(key) {
  const name = key.split("/").pop() || "";
  const index = name.lastIndexOf(".");
  if (index === -1) {
    return "";
  }
  return name.slice(index + 1).toLowerCase();
}

function objectUrl(bucket, key) {
  const encodedKey = key
    .split("/")
    .map((segment) => encodeURIComponent(segment))
    .join("/");
  return `/objects/${encodeURIComponent(bucket)}/${encodedKey}`;
}

function normalizeObjectKey(name) {
  const cleanName = name.trim().replace(/^\/+/, "");
  if (!cleanName) {
    return "";
  }

  return `${state.prefix}${cleanName}`;
}

function parentPrefix(prefix) {
  const parts = prefix.split("/").filter(Boolean);
  parts.pop();
  return parts.length === 0 ? "" : `${parts.join("/")}/`;
}

function trimTrailingSlash(value) {
  return value.endsWith("/") ? value.slice(0, -1) : value;
}

function formatBytes(size) {
  const units = ["B", "KiB", "MiB", "GiB", "TiB"];
  let value = Number(size);
  let unitIndex = 0;

  while (value >= 1024 && unitIndex < units.length - 1) {
    value /= 1024;
    unitIndex += 1;
  }

  const digits = unitIndex === 0 ? 0 : value >= 10 ? 1 : 2;
  return `${value.toFixed(digits)} ${units[unitIndex]}`;
}

function formatTimestamp(timestamp) {
  if (typeof timestamp !== "number") {
    return "-";
  }

  return new Date(timestamp * 1000).toLocaleString();
}

function setUploadProgress(percent, label) {
  elements.uploadProgressBar.style.width = `${Math.max(0, Math.min(percent, 100))}%`;
  elements.uploadProgressLabel.textContent = label;
}

function parseXhrError(xhr) {
  try {
    const payload = JSON.parse(xhr.responseText);
    return new Error(payload.message || payload.error || `Upload failed with status ${xhr.status}`);
  } catch (_error) {
    return new Error(`Upload failed with status ${xhr.status}`);
  }
}

function notify(message, tone = "info") {
  const toast = document.createElement("div");
  toast.className = `toast${tone === "info" ? "" : ` is-${tone}`}`;
  toast.textContent = message;
  elements.toastRack.appendChild(toast);
  window.setTimeout(() => {
    toast.remove();
  }, 2800);
}

function escapeHtml(value) {
  return String(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}
