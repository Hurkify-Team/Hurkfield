(function () {
  function humanSize(bytes) {
    const b = Number(bytes || 0);
    if (!b) return "0 B";
    if (b < 1024) return b + " B";
    if (b < 1024 * 1024) return (b / 1024).toFixed(1) + " KB";
    return (b / (1024 * 1024)).toFixed(2) + " MB";
  }

  function bindDropZone(zone, input) {
    if (!zone || !input) return;

    zone.addEventListener("click", function () {
      input.click();
    });
    zone.addEventListener("keydown", function (evt) {
      if (evt.key === "Enter" || evt.key === " ") {
        evt.preventDefault();
        input.click();
      }
    });
    zone.addEventListener("dragover", function (evt) {
      evt.preventDefault();
      zone.classList.add("drag");
    });
    zone.addEventListener("dragleave", function () {
      zone.classList.remove("drag");
    });
    zone.addEventListener("drop", function (evt) {
      evt.preventDefault();
      zone.classList.remove("drag");
      const files = evt.dataTransfer && evt.dataTransfer.files;
      if (!files || !files.length) return;
      try {
        const dt = new DataTransfer();
        dt.items.add(files[0]);
        input.files = dt.files;
      } catch (_) {
        // Some browsers block assignment. Fallback: trigger file picker.
      }
      input.dispatchEvent(new Event("change", { bubbles: true }));
    });
  }

  function bindNewInterviewUpload() {
    const form = document.getElementById("interviewForm");
    const fileInput = document.getElementById("audio_file_import");
    if (!form || !fileInput) return;

    const modeSelect = document.getElementById("interview_mode");
    const dropZone = document.getElementById("audioDropZone");
    const textEl = document.getElementById("audioUploadName");
    const progressWrap = document.getElementById("audioUploadProgress");
    const progressBar = document.getElementById("audioUploadBar");
    const pctEl = document.getElementById("audioUploadPct");
    const submitBtn = form.querySelector("button[type='submit']");

    bindDropZone(dropZone, fileInput);

    fileInput.addEventListener("change", function () {
      const f = fileInput.files && fileInput.files[0];
      if (!f) {
        if (textEl) textEl.textContent = "No file selected.";
        return;
      }
      if (textEl) textEl.textContent = f.name + " (" + humanSize(f.size) + ")";
      if (modeSelect && modeSelect.value !== "AUDIO") {
        modeSelect.value = "AUDIO";
        modeSelect.dispatchEvent(new Event("change", { bubbles: true }));
      }
    });

    form.addEventListener("submit", function (evt) {
      const f = fileInput.files && fileInput.files[0];
      const isAudioMode = modeSelect && modeSelect.value === "AUDIO";
      if (!f || !isAudioMode) return;
      if (form.dataset.uploading === "1") {
        evt.preventDefault();
        return;
      }
      if (!window.XMLHttpRequest || !window.FormData) return;

      evt.preventDefault();
      form.dataset.uploading = "1";
      if (submitBtn) submitBtn.disabled = true;
      if (progressWrap) progressWrap.classList.remove("hidden");
      if (pctEl) {
        pctEl.classList.remove("hidden");
        pctEl.textContent = "Uploading audio... 0%";
      }
      if (progressBar) progressBar.style.width = "0%";

      const xhr = new XMLHttpRequest();
      xhr.open("POST", form.getAttribute("action") || window.location.href, true);
      xhr.upload.addEventListener("progress", function (e) {
        if (!e.lengthComputable) return;
        const pct = Math.min(100, Math.round((e.loaded / e.total) * 100));
        if (progressBar) progressBar.style.width = pct + "%";
        if (pctEl) pctEl.textContent = "Uploading audio... " + pct + "%";
      });
      xhr.addEventListener("load", function () {
        form.dataset.uploading = "0";
        if (submitBtn) submitBtn.disabled = false;
        if (xhr.status >= 200 && xhr.status < 300) {
          document.open();
          document.write(xhr.responseText);
          document.close();
          return;
        }
        if (pctEl) pctEl.textContent = "Upload failed. Please try again.";
      });
      xhr.addEventListener("error", function () {
        form.dataset.uploading = "0";
        if (submitBtn) submitBtn.disabled = false;
        if (pctEl) pctEl.textContent = "Upload failed. Please check network and retry.";
      });
      xhr.send(new FormData(form));
    });
  }

  function bindInterviewDetailUpload() {
    const form = document.getElementById("intvAudioUploadForm");
    const input = document.getElementById("intvAudioUploadInput");
    if (!form || !input) return;

    const dropZone = document.getElementById("intvAudioDropZone");
    const progressWrap = document.getElementById("intvAudioUploadProgress");
    const progressBar = document.getElementById("intvAudioUploadBar");
    const textEl = document.getElementById("intvAudioUploadText");
    const submitBtn = document.getElementById("intvAudioUploadBtn");

    bindDropZone(dropZone, input);

    input.addEventListener("change", function () {
      const f = input.files && input.files[0];
      if (textEl) textEl.textContent = f ? (f.name + " (" + humanSize(f.size) + ")") : "No file selected.";
    });

    form.addEventListener("submit", function (evt) {
      const f = input.files && input.files[0];
      if (!f) return;
      if (form.dataset.uploading === "1") {
        evt.preventDefault();
        return;
      }
      if (!window.XMLHttpRequest || !window.FormData) return;

      evt.preventDefault();
      form.dataset.uploading = "1";
      if (submitBtn) submitBtn.disabled = true;
      if (progressWrap) progressWrap.classList.remove("hidden");
      if (progressBar) progressBar.style.width = "0%";
      if (textEl) textEl.textContent = "Uploading audio... 0%";

      const xhr = new XMLHttpRequest();
      xhr.open("POST", form.getAttribute("action") || window.location.href, true);
      xhr.upload.addEventListener("progress", function (e) {
        if (!e.lengthComputable) return;
        const pct = Math.min(100, Math.round((e.loaded / e.total) * 100));
        if (progressBar) progressBar.style.width = pct + "%";
        if (textEl) textEl.textContent = "Uploading audio... " + pct + "%";
      });
      xhr.addEventListener("load", function () {
        form.dataset.uploading = "0";
        if (submitBtn) submitBtn.disabled = false;
        if (xhr.status >= 200 && xhr.status < 300) {
          window.location.reload();
          return;
        }
        if (textEl) textEl.textContent = "Upload failed. Please try again.";
      });
      xhr.addEventListener("error", function () {
        form.dataset.uploading = "0";
        if (submitBtn) submitBtn.disabled = false;
        if (textEl) textEl.textContent = "Upload failed. Please check network and retry.";
      });
      xhr.send(new FormData(form));
    });
  }

  bindNewInterviewUpload();
  bindInterviewDetailUpload();
})();
