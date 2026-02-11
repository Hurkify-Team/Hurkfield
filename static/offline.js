(function () {
  const cfg = window.OPENFIELD_OFFLINE_CONFIG || {};
  const form = document.getElementById("openfieldForm");
  if (!cfg.syncUrl) {
    return;
  }

  const syncCard = document.getElementById("offlineSyncCard");
  if (!form && !syncCard) {
    return;
  }
  const clientUuidInput = document.getElementById("clientUuidInput");
  const clientCreatedInput = document.getElementById("clientCreatedAtInput");
  const submitBtn = document.getElementById("submitBtn");
  const submitHint = document.getElementById("submitHint");

  const storageKey = "openfield_offline_queue:" + (cfg.queueKey || cfg.syncUrl);

  function nowIso() {
    return new Date().toISOString();
  }

  function makeUuid() {
    if (window.crypto && crypto.randomUUID) {
      return crypto.randomUUID();
    }
    return "of_" + Math.random().toString(36).slice(2) + "_" + Date.now().toString(36);
  }

  function loadQueue() {
    try {
      const raw = localStorage.getItem(storageKey);
      const parsed = raw ? JSON.parse(raw) : [];
      return Array.isArray(parsed) ? parsed : [];
    } catch (e) {
      return [];
    }
  }

  function saveQueue(items) {
    try {
      localStorage.setItem(storageKey, JSON.stringify(items || []));
    } catch (e) {
      // ignore
    }
  }

  function ensureClientIds() {
    if (clientUuidInput && !clientUuidInput.value) {
      clientUuidInput.value = makeUuid();
    }
    if (clientCreatedInput && !clientCreatedInput.value) {
      clientCreatedInput.value = nowIso();
    }
  }

  function serializeForm() {
    const data = {};
    const fd = new FormData(form);
    for (const [key, val] of fd.entries()) {
      if (data[key] === undefined) {
        data[key] = val;
      } else if (Array.isArray(data[key])) {
        data[key].push(val);
      } else {
        data[key] = [data[key], val];
      }
    }
    return data;
  }

  function getFacilityLabel(fields) {
    const facilityId = fields.facility_id;
    if (facilityId) {
      const select = form.querySelector("select[name='facility_id']");
      if (select) {
        const option = select.querySelector(`option[value="${CSS.escape(String(facilityId))}"]`);
        if (option && option.textContent) {
          return option.textContent.trim();
        }
      }
    }
    return (fields.facility_name || "").toString();
  }

  function queueSubmission(fields) {
    const queuedAt = nowIso();
    const entry = {
      id: fields.client_uuid || makeUuid(),
      queued_at: queuedAt,
      status: "pending",
      attempts: 0,
      last_error: "",
      meta: {
        enumerator: fields.enumerator_name || "",
        facility: getFacilityLabel(fields),
        queued_at: queuedAt,
      },
      fields: fields,
    };
    const queue = loadQueue();
    queue.push(entry);
    saveQueue(queue);
    renderSyncCenter();
  }

  function setSubmitState(msg) {
    if (submitBtn) {
      submitBtn.disabled = false;
      submitBtn.innerText = "Submit";
    }
    if (submitHint && msg) {
      submitHint.innerText = msg;
    }
  }

  function queueFromForm() {
    if (!form) return;
    ensureClientIds();
    const fields = serializeForm();
    fields.client_uuid = clientUuidInput ? clientUuidInput.value : fields.client_uuid;
    fields.client_created_at = clientCreatedInput ? clientCreatedInput.value : fields.client_created_at || nowIso();
    fields.sync_source = "OFFLINE_SYNC";
    queueSubmission(fields);
    if (clientUuidInput) clientUuidInput.value = makeUuid();
    if (clientCreatedInput) clientCreatedInput.value = nowIso();
    setSubmitState("Saved offline. Sync when you have internet.");
  }

  async function syncEntry(entry, queue) {
    entry.status = "syncing";
    entry.attempts = (entry.attempts || 0) + 1;
    entry.last_error = "";
    saveQueue(queue);
    renderSyncCenter();
    try {
      const res = await fetch(cfg.syncUrl, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ submission: entry.fields }),
      });
      const data = await res.json().catch(() => ({}));
      if (!res.ok || !data.ok) {
        throw new Error(data.error || "Sync failed");
      }
      return true;
    } catch (err) {
      entry.status = "error";
      entry.last_error = err && err.message ? err.message : "Sync failed";
      return false;
    }
  }

  async function syncAll() {
    const queue = loadQueue();
    if (!queue.length) {
      renderSyncCenter();
      return;
    }
    for (let i = queue.length - 1; i >= 0; i -= 1) {
      const entry = queue[i];
      if (entry.status === "syncing") {
        continue;
      }
      const ok = await syncEntry(entry, queue);
      if (ok) {
        queue.splice(i, 1);
      }
      saveQueue(queue);
      renderSyncCenter();
    }
  }

  function renderSyncCenter() {
    if (!syncCard) return;
    const queue = loadQueue();
    const online = navigator.onLine;
    if (!queue.length && online) {
      syncCard.style.display = "none";
      return;
    }
    syncCard.style.display = "block";

    const listHtml = queue.length
      ? queue
          .map((entry) => {
            const status = entry.status || "pending";
            const pillClass = status === "error" ? "error" : status === "syncing" ? "syncing" : "pending";
            const meta = entry.meta || {};
            const title = `${meta.facility || "Facility"} - ${meta.enumerator || "Enumerator"}`;
            const subtitle = meta.queued_at ? new Date(meta.queued_at).toLocaleString() : "";
            const errorText = entry.last_error ? `<div class="meta">Error: ${entry.last_error}</div>` : "";
            return `
              <div class="offline-item">
                <div>
                  <div style="font-weight:700">${title}</div>
                  <div class="meta">${subtitle}</div>
                  ${errorText}
                </div>
                <span class="offline-pill ${pillClass}">${status}</span>
              </div>
            `;
          })
          .join("")
      : `<div class="muted">No offline submissions pending.</div>`;

    const syncLink = cfg.syncCenterUrl ? `<a class="btn sm" href="${cfg.syncCenterUrl}">Open sync center</a>` : "";
    syncCard.innerHTML = `
      <div class="offline-head">
        <div>
          <div class="offline-title">Offline Sync Center</div>
          <div class="offline-sub">Submissions saved on this device will sync when you are back online.</div>
        </div>
        <div class="offline-status ${online ? "online" : "offline"}">${online ? "Online" : "Offline"}</div>
      </div>
      <div class="offline-actions">
        <button class="btn sm" type="button" id="offlineSyncBtn" ${online && queue.length ? "" : "disabled"}>Sync now</button>
        <button class="btn sm" type="button" id="offlineClearBtn" ${queue.length ? "" : "disabled"}>Clear queue</button>
        ${syncLink}
      </div>
      <div class="offline-list">${listHtml}</div>
    `;

    const syncBtn = document.getElementById("offlineSyncBtn");
    const clearBtn = document.getElementById("offlineClearBtn");
    if (syncBtn) {
      syncBtn.addEventListener("click", () => syncAll());
    }
    if (clearBtn) {
      clearBtn.addEventListener("click", () => {
        if (queue.length && window.confirm("Clear all pending offline submissions?")) {
          saveQueue([]);
          renderSyncCenter();
        }
      });
    }
  }

  ensureClientIds();
  renderSyncCenter();

  window.addEventListener("online", () => {
    renderSyncCenter();
    syncAll();
  });
  window.addEventListener("offline", () => renderSyncCenter());

  window.OPENFIELD_OFFLINE_QUEUE = {
    queueFromForm,
    syncAll,
  };
})();
