/* Template builder UI logic (Google-Forms-style switching) */
(function () {
  const cfg = window.BUILDER_CONFIG || {};
  const list = document.querySelector(".gf-list");
  if (!list) return;

  const choiceTypes = new Set(["SINGLE_CHOICE", "MULTI_CHOICE", "DROPDOWN", "YESNO"]);

  function questionUrl(base, qid) {
    return (base || "").replace("/0/", `/${qid}/`);
  }

  function showToast(msg) {
    let toast = document.getElementById("builderToast");
    if (!toast) {
      toast = document.createElement("div");
      toast.id = "builderToast";
      toast.style.position = "fixed";
      toast.style.right = "18px";
      toast.style.bottom = "18px";
      toast.style.padding = "10px 14px";
      toast.style.background = "var(--primary)";
      toast.style.color = "#fff";
      toast.style.borderRadius = "10px";
      toast.style.boxShadow = "0 10px 24px rgba(15,18,34,.25)";
      toast.style.zIndex = "9999";
      document.body.appendChild(toast);
    }
    toast.textContent = msg;
    toast.style.display = "block";
    setTimeout(() => {
      toast.style.display = "none";
    }, 3000);
  }

  function applyRich(command) {
    const active = document.activeElement;
    if (!active || !active.classList || !active.classList.contains("rich-text")) return;
    if (command === "link") {
      const url = prompt("Enter link URL");
      if (!url) return;
      document.execCommand("createLink", false, url);
      return;
    }
    if (command === "clear") {
      document.execCommand("removeFormat", false, null);
      return;
    }
    document.execCommand(command, false, null);
  }

  function getType(card) {
    const sel = card.querySelector(".q-type");
    return (sel && sel.value ? sel.value : "TEXT").trim().toUpperCase();
  }

  function setHint(card, type) {
    const hint = card.querySelector(".q-hint");
    if (!hint) return;
    if (type === "TEXT") hint.textContent = "Short answer text";
    else if (type === "LONGTEXT") hint.textContent = "Paragraph answer text";
    else if (choiceTypes.has(type)) hint.textContent = "Add options below";
    else hint.textContent = "";
  }

  function ensureOptionRow(listEl, value) {
    const row = document.createElement("div");
    row.className = "q-option";
    row.innerHTML =
      '<span class="q-option-dot"></span>' +
      '<input class="q-option-input" placeholder="Option" value="' +
      (value || "") +
      '" />' +
      '<button class="q-option-remove" type="button">✕</button>';
    listEl.appendChild(row);
  }

  function addOptionByCard(card, value) {
    const listEl = card.querySelector(".q-options-list");
    if (!listEl) return;
    ensureOptionRow(listEl, value);
    renderPreview(card, getType(card));
  }

  function ensureOptions(card, type) {
    const optionsWrap = card.querySelector(".q-options");
    if (!optionsWrap) return;
    const listEl = optionsWrap.querySelector(".q-options-list");
    const addBtns = optionsWrap.querySelectorAll(".q-add-option");
    const show = choiceTypes.has(type);

    optionsWrap.style.display = show ? "grid" : "none";
    addBtns.forEach((btn) => {
      btn.style.display = ["SINGLE_CHOICE", "MULTI_CHOICE", "DROPDOWN"].includes(type)
        ? "inline-flex"
        : "none";
    });
    const addRow = optionsWrap.querySelector(".q-options-list")?.nextElementSibling;
    if (addRow && addRow.classList && addRow.classList.contains("row")) {
      addRow.style.display = ["SINGLE_CHOICE", "MULTI_CHOICE", "DROPDOWN"].includes(type)
        ? "flex"
        : "none";
    }

    if (!show || !listEl) return;
    if (type === "YESNO") {
      listEl.innerHTML = "<div class='muted'>Yes / No</div>";
      return;
    }
    if (listEl.querySelectorAll(".q-option").length === 0) {
      listEl.innerHTML = "";
      ensureOptionRow(listEl, "Option 1");
      ensureOptionRow(listEl, "Option 2");
    }
  }

  function getOptions(card) {
    return Array.from(card.querySelectorAll(".q-option-input"))
      .map((i) => i.value.trim())
      .filter(Boolean);
  }

  function renderPreview(card, type) {
    const preview = card.querySelector(".q-preview");
    if (!preview) return;
    const options = getOptions(card);
    let html = "";
    if (type === "LONGTEXT") {
      html = '<textarea class="q-preview-input" rows="2" placeholder="Long answer text"></textarea>';
    } else if (type === "NUMBER") {
      html = '<input class="q-preview-input" type="number" placeholder="Enter a number" />';
    } else if (type === "DATE") {
      html = '<input class="q-preview-input" type="date" />';
    } else if (type === "EMAIL") {
      html = '<input class="q-preview-input" type="email" placeholder="name@example.com" />';
    } else if (type === "PHONE") {
      html = '<input class="q-preview-input" type="tel" placeholder="Phone number" />';
    } else if (type === "YESNO") {
      html =
        '<div class="q-preview-options">' +
        '<label class="q-option-line"><input type="radio" name="yn_preview" /><span>Yes</span></label>' +
        '<label class="q-option-line"><input type="radio" name="yn_preview" /><span>No</span></label>' +
        "</div>";
    } else if (type === "SINGLE_CHOICE") {
      html = options.length
        ? '<div class="q-preview-options">' +
          options
            .map(
              (o) =>
                '<label class="q-option-line"><input type="radio" name="opt_preview" /><span>' +
                o +
                "</span></label>"
            )
            .join("") +
          "</div>"
        : '<div class="muted">Add options below.</div>';
    } else if (type === "MULTI_CHOICE") {
      html = options.length
        ? '<div class="q-preview-options">' +
          options
            .map(
              (o) =>
                '<label class="q-option-line"><input type="checkbox" /><span>' +
                o +
                "</span></label>"
            )
            .join("") +
          "</div>"
        : '<div class="muted">Add options below.</div>';
    } else if (type === "DROPDOWN") {
      html =
        '<select class="q-preview-input">' +
        '<option selected disabled>Select option</option>' +
        (options.length ? options.map((o) => `<option>${o}</option>`).join("") : "<option>Add options below</option>") +
        "</select>";
    } else {
      html = '<input class="q-preview-input" placeholder="Short answer text" />';
    }
    preview.innerHTML = html;
  }

  function syncCard(card) {
    const type = getType(card);
    setHint(card, type);
    ensureOptions(card, type);
    renderPreview(card, type);
  }

  function initCard(card) {
    if (!card) return;
    card.addEventListener("click", (e) => {
      if (e.target.closest("input, textarea, select, button, a")) return;
      card.classList.add("editing");
      card.scrollIntoView({ behavior: "smooth", block: "center" });
      const input = card.querySelector(".q-text");
      if (input) input.focus();
      setTimeout(() => card.classList.remove("editing"), 1200);
    });
    syncCard(card);
  }

  function buildQuestionCard(qid, orderNo) {
    const card = document.createElement("div");
    card.className = "gf-card";
    card.dataset.qid = String(qid);
    card.draggable = true;
    const delUrl = questionUrl(cfg.deleteUrlBase, qid);
    const dupUrl = questionUrl(cfg.duplicateUrlBase, qid);
    card.innerHTML =
      '<div class="gf-card-head">' +
      '<div class="gf-order">' +
      '<span class="drag-handle" title="Drag to reorder">⋮⋮</span>' +
      '<span class="order-pill">' +
      orderNo +
      "</span>" +
      "</div>" +
      '<div class="gf-type">' +
      '<select class="q-type">' +
      '<option value="TEXT" selected>Short answer</option>' +
      '<option value="LONGTEXT">Paragraph</option>' +
      '<option value="SINGLE_CHOICE">Multiple choice</option>' +
      '<option value="MULTI_CHOICE">Checkboxes</option>' +
      '<option value="DROPDOWN">Dropdown</option>' +
      '<option value="YESNO">Yes / No</option>' +
      '<option value="NUMBER">Number</option>' +
      '<option value="DATE">Date</option>' +
      '<option value="EMAIL">Email</option>' +
      '<option value="PHONE">Phone</option>' +
      "</select>" +
      "</div>" +
      "</div>" +
      '<div class="gf-question">' +
      '<div class="q-text rich-text" contenteditable="true" data-placeholder="Untitled Question">Untitled Question</div>' +
      '<div class="q-hint">Short answer text</div>' +
      '<div class="q-preview"></div>' +
      '<div class="q-options" style="display:none">' +
      '<div class="q-options-label">Options</div>' +
      '<div class="q-options-list"></div>' +
      '<div class="row" style="gap:8px; display:none;">' +
      '<button class="btn btn-sm btn-ghost q-add-option" data-action="add" type="button">Add option</button>' +
      '<button class="btn btn-sm btn-ghost q-add-option" data-action="other" type="button">Add “Other”</button>' +
      "</div>" +
      "</div>" +
      "</div>" +
      '<div class="gf-card-actions">' +
      '<div class="row" style="gap:10px;">' +
      '<button class="btn btn-sm" type="button" data-save-question>Save</button>' +
      '<a class="btn btn-sm" href="' +
      dupUrl +
      '">Duplicate</a>' +
      '<a class="btn btn-sm" data-delete="1" href="' +
      delUrl +
      '" onclick="return confirm(\'Delete this question?\')">Delete</a>' +
      "</div>" +
      '<label class="row" style="gap:8px;">' +
      '<span class="muted">Required</span>' +
      '<input class="q-required" type="checkbox" />' +
      "</label>" +
      "</div>";
    return card;
  }

  async function addBlankQuestion() {
    if (!cfg.quickAddUrl) return;
    try {
      const res = await fetch(cfg.quickAddUrl, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          question_text: "Untitled Question",
          question_type: "TEXT",
          is_required: false,
          choices: [],
        }),
      });
      const data = await res.json();
      if (!data.ok || !data.question_id) throw new Error(data.error || "Save failed");
      const orderNo = list.querySelectorAll(".gf-card[data-qid]").length + 1;
      const card = buildQuestionCard(data.question_id, orderNo);
      list.appendChild(card);
      initCard(card);
      syncCard(card);
      card.scrollIntoView({ behavior: "smooth", block: "center" });
      showToast("Question added");
    } catch (e) {
      window.location.href = cfg.quickAddUrl;
    }
  }

  async function saveQuestion(card) {
    if (!cfg.inlineUpdateBase) return;
    const qid = card.dataset.qid;
    const textEl = card.querySelector(".q-text");
    const text = textEl ? textEl.innerHTML : "";
    const type = getType(card);
    const required = (card.querySelector(".q-required") || {}).checked || false;
    const choices = getOptions(card);
    const url = questionUrl(cfg.inlineUpdateBase, qid);
    const res = await fetch(url, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        question_text: text,
        question_type: type,
        is_required: required,
        choices,
      }),
    });
    const data = await res.json();
    if (!data.ok) {
      alert(data.error || "Save failed");
      return;
    }
    showToast("Saved");
  }

  // Back-compat helpers (inline onclick from server-rendered HTML)
  window.addOption = function (qid) {
    const card = document.querySelector(`.gf-card[data-qid="${qid}"]`);
    if (card) addOptionByCard(card, "");
  };

  window.addOtherOption = function (qid) {
    const card = document.querySelector(`.gf-card[data-qid="${qid}"]`);
    if (card) addOptionByCard(card, "Other");
  };

  window.removeOption = function (btn) {
    const row = btn.closest(".q-option");
    if (row) row.remove();
    const card = btn.closest(".gf-card");
    if (card) renderPreview(card, getType(card));
  };

  window.saveQuestion = function (qid) {
    const card = document.querySelector(`.gf-card[data-qid="${qid}"]`);
    if (card) saveQuestion(card);
  };

  // Drag reorder
  let dragging = null;
  function persistOrder() {
    if (!cfg.reorderUrl) return;
    const order = Array.from(list.querySelectorAll(".gf-card[data-qid]")).map((row) => row.dataset.qid);
    fetch(cfg.reorderUrl, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ order }),
    })
      .then((res) => res.json())
      .then((data) => {
        if (data && data.ok) {
          Array.from(list.querySelectorAll(".gf-card[data-qid]")).forEach((row, idx) => {
            const cell = row.querySelector(".order-pill");
            if (cell) cell.textContent = String(idx + 1);
          });
        }
      })
      .catch(() => {});
  }

  list.addEventListener("dragover", (e) => {
    e.preventDefault();
    const target = e.target.closest(".gf-card");
    if (!target || target === dragging) return;
    const rect = target.getBoundingClientRect();
    const next = e.clientY - rect.top > rect.height / 2;
    list.insertBefore(dragging, next ? target.nextSibling : target);
  });

  list.addEventListener("dragstart", (e) => {
    const card = e.target.closest(".gf-card");
    if (!card) return;
    dragging = card;
    dragging.classList.add("dragging");
  });

  list.addEventListener("dragend", () => {
    if (dragging) dragging.classList.remove("dragging");
    dragging = null;
    persistOrder();
  });

  // Delegated events
  document.addEventListener("change", (e) => {
    const typeSel = e.target.closest && e.target.closest(".q-type");
    if (typeSel) {
      const card = typeSel.closest(".gf-card");
      if (card) syncCard(card);
    }
  });

  document.addEventListener("input", (e) => {
    if (e.target && e.target.classList && e.target.classList.contains("q-option-input")) {
      const card = e.target.closest(".gf-card");
      if (card) renderPreview(card, getType(card));
    }
  });

  document.addEventListener("click", (e) => {
    const delLink = e.target.closest && e.target.closest("a[data-delete]");
    if (delLink) {
      try {
        sessionStorage.setItem("openfield_builder_scroll", String(window.scrollY || 0));
      } catch (err) {}
      return;
    }
    const richBtn = e.target.closest && e.target.closest("[data-rich]");
    if (richBtn) {
      e.preventDefault();
      const cmd = richBtn.getAttribute("data-rich");
      applyRich(cmd);
      return;
    }
    const addBtn = e.target.closest && e.target.closest("[data-add-question]");
    if (addBtn) {
      e.preventDefault();
      addBlankQuestion();
      return;
    }
    const optBtn = e.target.closest && e.target.closest(".q-add-option");
    if (optBtn) {
      e.preventDefault();
      const card = optBtn.closest(".gf-card");
      const listEl = card && card.querySelector(".q-options-list");
      if (!card || !listEl) return;
      const action = optBtn.getAttribute("data-action") || "add";
      ensureOptionRow(listEl, action === "other" ? "Other" : "");
      renderPreview(card, getType(card));
      return;
    }
    const removeBtn = e.target.closest && e.target.closest(".q-option-remove");
    if (removeBtn) {
      const row = removeBtn.closest(".q-option");
      if (row) row.remove();
      const card = e.target.closest(".gf-card");
      if (card) renderPreview(card, getType(card));
      return;
    }
    const saveBtn = e.target.closest && e.target.closest("[data-save-question]");
    if (saveBtn) {
      const card = saveBtn.closest(".gf-card");
      if (card) saveQuestion(card);
    }
  });

  // Section + media helpers
  window.openSectionModal = function (title, desc, qid) {
    const modal = document.getElementById("sectionModal");
    if (!modal) return;
    const titleEl = document.getElementById("sectionTitle");
    const descEl = document.getElementById("sectionDesc");
    const idEl = document.getElementById("sectionId");
    if (titleEl) titleEl.value = title || "";
    if (descEl) descEl.value = desc || "";
    if (idEl) idEl.value = qid || "";
    modal.classList.add("show");
  };

  window.closeSectionModal = function () {
    const modal = document.getElementById("sectionModal");
    if (modal) modal.classList.remove("show");
  };

  window.addSectionMarker = function () {
    window.openSectionModal();
  };

  window.saveSection = async function () {
    const titleEl = document.getElementById("sectionTitle");
    const descEl = document.getElementById("sectionDesc");
    const idEl = document.getElementById("sectionId");
    const title = (titleEl && titleEl.value) || "";
    const desc = (descEl && descEl.value) || "";
    const qid = (idEl && idEl.value) || "";
    if (!title.trim()) {
      alert("Section title is required.");
      return;
    }
    const formData = new FormData();
    formData.append("section_title", title.trim());
    formData.append("section_desc", desc.trim());
    let url = cfg.sectionAddUrl;
    if (qid) url = questionUrl(cfg.sectionUpdateBase, qid);
    await fetch(url, { method: "POST", body: formData });
    window.location.reload();
  };

  window.addMediaBlock = function (kind) {
    const useUpload = confirm("Upload a file? Click OK to upload, Cancel to paste a URL.");
    if (useUpload) {
      uploadMedia(kind);
      return;
    }
    const label = kind === "image" ? "Image URL" : "Video URL";
    const url = prompt(label);
    if (!url) return;
    const caption = prompt("Caption (optional)") || "";
    const formData = new FormData();
    formData.append("media_type", kind);
    formData.append("media_url", url);
    formData.append("media_caption", caption);
    fetch(cfg.mediaAddUrl, { method: "POST", body: formData }).then(() => window.location.reload());
  };

  function uploadMedia(kind) {
    const input = document.createElement("input");
    input.type = "file";
    input.accept = kind === "image" ? "image/*" : "video/*";
    input.onchange = async (e) => {
      const file = e.target.files && e.target.files[0];
      if (!file) return;
      const formData = new FormData();
      formData.append("media", file);
      const res = await fetch(cfg.mediaUploadUrl, { method: "POST", body: formData });
      const data = await res.json();
      if (!data.ok) {
        alert(data.error || "Upload failed");
        return;
      }
      const caption = prompt("Caption (optional)") || "";
      const addForm = new FormData();
      addForm.append("media_type", kind);
      addForm.append("media_url", data.url);
      addForm.append("media_caption", caption);
      await fetch(cfg.mediaAddUrl, { method: "POST", body: addForm });
      window.location.reload();
    };
    input.click();
  }

  // Bind existing cards
  list.querySelectorAll(".gf-card[data-qid]").forEach(initCard);

  try {
    const saved = sessionStorage.getItem("openfield_builder_scroll");
    if (saved) {
      sessionStorage.removeItem("openfield_builder_scroll");
      const y = parseInt(saved, 10);
      if (!Number.isNaN(y)) {
        window.scrollTo({ top: y, behavior: "instant" });
      }
    }
  } catch (err) {}
})();
