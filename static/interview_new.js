(function () {
  const form = document.getElementById("interviewForm");
  if (!form) return;

  const modeSelect = document.getElementById("interview_mode");
  const consentSelect = document.getElementById("consent_obtained");
  const audioAllowedField = document.getElementById("audioAllowedField");
  const audioAllowedSelect = document.getElementById("audio_recording_allowed");
  const notePanel = document.getElementById("noteModePanel");
  const audioPanel = document.getElementById("audioModePanel");
  const notesInput = document.getElementById("interview_text");
  const audioDataInput = document.getElementById("audio_data_url");
  const audioFileInput = document.getElementById("audio_file_import");
  const audioUploadName = document.getElementById("audioUploadName");

  const startBtn = document.getElementById("audioStartBtn");
  const pauseBtn = document.getElementById("audioPauseBtn");
  const resumeBtn = document.getElementById("audioResumeBtn");
  const stopBtn = document.getElementById("audioStopBtn");
  const clearBtn = document.getElementById("audioClearBtn");
  const audioPreview = document.getElementById("audioPreview");
  const timerEl = document.getElementById("audioTimer");
  const statusEl = document.getElementById("audioStatusText");
  const errorEl = document.getElementById("audioErrorText");

  let mediaStream = null;
  let mediaRecorder = null;
  let chunks = [];
  let timerHandle = null;
  let startEpoch = 0;
  let elapsedBeforePause = 0;
  let isRecording = false;
  let isPaused = false;
  let uploadedPreviewUrl = null;

  function fmt(ms) {
    const total = Math.max(0, Math.floor(ms / 1000));
    const mm = String(Math.floor(total / 60)).padStart(2, "0");
    const ss = String(total % 60).padStart(2, "0");
    return mm + ":" + ss;
  }

  function updateTimer() {
    if (!isRecording) {
      timerEl.textContent = fmt(elapsedBeforePause);
      return;
    }
    const elapsed = elapsedBeforePause + (Date.now() - startEpoch);
    timerEl.textContent = fmt(elapsed);
  }

  function startTimerLoop() {
    stopTimerLoop();
    timerHandle = setInterval(updateTimer, 250);
    updateTimer();
  }

  function stopTimerLoop() {
    if (timerHandle) {
      clearInterval(timerHandle);
      timerHandle = null;
    }
  }

  function setStatus(msg) {
    if (statusEl) statusEl.textContent = msg;
  }

  function setError(msg) {
    if (!errorEl) return;
    if (msg) {
      errorEl.textContent = msg;
      errorEl.classList.remove("hidden");
    } else {
      errorEl.textContent = "";
      errorEl.classList.add("hidden");
    }
  }

  function setControls() {
    startBtn.disabled = isRecording || isPaused;
    pauseBtn.disabled = !isRecording || isPaused;
    resumeBtn.disabled = !isPaused;
    stopBtn.disabled = !(isRecording || isPaused);
    const hasUploadedFile = !!(audioFileInput && audioFileInput.files && audioFileInput.files.length);
    clearBtn.disabled = !audioDataInput.value && !hasUploadedFile && !isRecording && !isPaused;
  }

  async function ensureStream() {
    if (mediaStream) return mediaStream;
    if (!navigator.mediaDevices || !navigator.mediaDevices.getUserMedia) {
      throw new Error("Audio recording is not supported in this browser.");
    }
    mediaStream = await navigator.mediaDevices.getUserMedia({ audio: true });
    return mediaStream;
  }

  function pickMimeType() {
    if (!window.MediaRecorder) return "";
    const prefs = [
      "audio/webm;codecs=opus",
      "audio/webm",
      "audio/ogg;codecs=opus",
      "audio/mp4"
    ];
    for (const mime of prefs) {
      try {
        if (MediaRecorder.isTypeSupported(mime)) return mime;
      } catch (_) {
        // ignore
      }
    }
    return "";
  }

  async function startRecording(autoRequested) {
    try {
      setError("");
      if (consentSelect.value !== "YES") {
        throw new Error("Set Consent obtained to Yes before recording.");
      }
      if (audioAllowedSelect.value !== "YES") {
        audioAllowedSelect.value = "YES";
      }

      const stream = await ensureStream();
      chunks = [];

      const mime = pickMimeType();
      mediaRecorder = mime ? new MediaRecorder(stream, { mimeType: mime }) : new MediaRecorder(stream);
      mediaRecorder.ondataavailable = function (evt) {
        if (evt.data && evt.data.size > 0) chunks.push(evt.data);
      };
      mediaRecorder.onstop = function () {
        const type = (mediaRecorder && mediaRecorder.mimeType) || "audio/webm";
        if (!chunks.length) {
          setStatus("No audio captured");
          setControls();
          return;
        }
        const blob = new Blob(chunks, { type: type });
        const url = URL.createObjectURL(blob);
        audioPreview.src = url;
        audioPreview.classList.remove("hidden");
        const reader = new FileReader();
        reader.onloadend = function () {
          audioDataInput.value = String(reader.result || "");
          clearBtn.disabled = false;
        };
        reader.readAsDataURL(blob);
        setStatus("Recording saved");
        setControls();
      };

      mediaRecorder.start(1000);
      isRecording = true;
      isPaused = false;
      startEpoch = Date.now();
      elapsedBeforePause = 0;
      startTimerLoop();
      setStatus(autoRequested ? "Recording started" : "Recording");
      setControls();
    } catch (err) {
      setError((err && err.message) || "Unable to start recording.");
      setStatus("Recorder unavailable");
    }
  }

  function pauseRecording() {
    if (!mediaRecorder || !isRecording || isPaused) return;
    mediaRecorder.pause();
    isPaused = true;
    isRecording = false;
    elapsedBeforePause += Date.now() - startEpoch;
    stopTimerLoop();
    updateTimer();
    setStatus("Paused");
    setControls();
  }

  function resumeRecording() {
    if (!mediaRecorder || !isPaused) return;
    mediaRecorder.resume();
    isPaused = false;
    isRecording = true;
    startEpoch = Date.now();
    startTimerLoop();
    setStatus("Recording");
    setControls();
  }

  function stopRecording() {
    if (!mediaRecorder || (!isRecording && !isPaused)) return;
    if (isRecording) {
      elapsedBeforePause += Date.now() - startEpoch;
    }
    isRecording = false;
    isPaused = false;
    stopTimerLoop();
    updateTimer();
    try {
      mediaRecorder.stop();
    } catch (_) {
      // ignore
    }
    setStatus("Processing audio...");
    setControls();
  }

  function clearRecording() {
    if (isRecording || isPaused) stopRecording();
    audioDataInput.value = "";
    if (audioFileInput) {
      audioFileInput.value = "";
    }
    if (uploadedPreviewUrl) {
      URL.revokeObjectURL(uploadedPreviewUrl);
      uploadedPreviewUrl = null;
    }
    audioPreview.removeAttribute("src");
    audioPreview.classList.add("hidden");
    elapsedBeforePause = 0;
    updateTimer();
    setStatus(modeSelect && modeSelect.value === "AUDIO" ? "Choose record or import" : "Idle");
    if (audioUploadName) audioUploadName.textContent = "No file selected.";
    setError("");
    setControls();
  }

  function handleImportedAudio() {
    if (!audioFileInput) return;
    const file = (audioFileInput.files && audioFileInput.files[0]) || null;
    if (!file) {
      if (audioUploadName) audioUploadName.textContent = "No file selected.";
      if (!audioDataInput.value) {
        if (uploadedPreviewUrl) {
          URL.revokeObjectURL(uploadedPreviewUrl);
          uploadedPreviewUrl = null;
        }
        audioPreview.removeAttribute("src");
        audioPreview.classList.add("hidden");
        setStatus(modeSelect && modeSelect.value === "AUDIO" ? "Choose record or import" : "Idle");
      }
      setControls();
      return;
    }

    if (isRecording || isPaused) {
      stopRecording();
    }
    if (modeSelect && modeSelect.value !== "AUDIO") {
      modeSelect.value = "AUDIO";
      applyMode(false);
    }
    audioDataInput.value = "";
    if (uploadedPreviewUrl) {
      URL.revokeObjectURL(uploadedPreviewUrl);
      uploadedPreviewUrl = null;
    }
    uploadedPreviewUrl = URL.createObjectURL(file);
    audioPreview.src = uploadedPreviewUrl;
    audioPreview.classList.remove("hidden");
    if (audioUploadName) {
      const sizeMb = file.size ? (file.size / (1024 * 1024)).toFixed(2) : "0.00";
      audioUploadName.textContent = file.name + " (" + sizeMb + " MB)";
    }
    setStatus("Imported audio ready");
    setError("");
    if (audioAllowedSelect.value !== "YES") {
      audioAllowedSelect.value = "YES";
    }
    setControls();
  }

  function applyMode(autoStart) {
    const mode = modeSelect.value === "AUDIO" ? "AUDIO" : "TEXT";
    if (mode === "AUDIO") {
      audioPanel.classList.remove("hidden");
      notePanel.classList.add("hidden");
      audioAllowedField.classList.remove("hidden");
      if (audioAllowedSelect.value !== "YES") audioAllowedSelect.value = "YES";
      if (!audioDataInput.value && !(audioFileInput && audioFileInput.files && audioFileInput.files.length)) {
        setStatus("Choose record or import");
      }
    } else {
      notePanel.classList.remove("hidden");
      audioPanel.classList.add("hidden");
      audioAllowedField.classList.add("hidden");
      audioAllowedSelect.value = "NO";
      if (isRecording || isPaused) {
        stopRecording();
      }
      setError("");
    }
    setControls();
  }

  startBtn.addEventListener("click", function () { startRecording(false); });
  pauseBtn.addEventListener("click", pauseRecording);
  resumeBtn.addEventListener("click", resumeRecording);
  stopBtn.addEventListener("click", stopRecording);
  clearBtn.addEventListener("click", clearRecording);

  modeSelect.addEventListener("change", function () { applyMode(false); });

  consentSelect.addEventListener("change", function () {
    if (modeSelect.value === "AUDIO" && consentSelect.value !== "YES" && (isRecording || isPaused)) {
      stopRecording();
      setError("Recording stopped: set Consent to Yes before continuing.");
    }
  });

  form.addEventListener("submit", function (evt) {
    setError("");
    const mode = modeSelect.value === "AUDIO" ? "AUDIO" : "TEXT";

    if (mode === "AUDIO") {
      if (consentSelect.value !== "YES") {
        evt.preventDefault();
        setError("Consent must be set to Yes before audio submission.");
        return;
      }
      if (audioAllowedSelect.value !== "YES") {
        evt.preventDefault();
        setError("Set Audio recording allowed to Yes or switch to text mode.");
        return;
      }
      const hasUploadedFile = !!(audioFileInput && audioFileInput.files && audioFileInput.files.length);
      if (!audioDataInput.value && !hasUploadedFile) {
        evt.preventDefault();
        setError("Record audio or import an audio file before submitting.");
        return;
      }
      if (isRecording || isPaused) {
        evt.preventDefault();
        setError("Stop recording before submitting.");
        return;
      }
    } else if (!notesInput.value.trim()) {
      evt.preventDefault();
      setError("Interview notes are required in text mode.");
      notesInput.focus();
    }
  });

  if (audioFileInput) {
    audioFileInput.addEventListener("change", handleImportedAudio);
  }

  window.addEventListener("beforeunload", function () {
    try {
      if (mediaRecorder && (isRecording || isPaused)) mediaRecorder.stop();
      if (mediaStream) {
        mediaStream.getTracks().forEach(function (t) { t.stop(); });
      }
    } catch (_) {
      // ignore
    }
  });

  if (audioDataInput.value) {
    audioPreview.classList.remove("hidden");
    clearBtn.disabled = false;
    setStatus("Audio attached");
  } else if (audioFileInput && audioFileInput.files && audioFileInput.files.length) {
    handleImportedAudio();
  } else {
    setStatus("Idle");
  }

  applyMode(false);
})();
