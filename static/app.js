/* ============================================================
   Linux RAG Debugger — Frontend (SSE Streaming + SVG Icons)
   ============================================================ */

const messagesEl = document.getElementById("messages");
const welcomeEl = document.getElementById("welcome");
const inputEl = document.getElementById("userInput");
const sendBtn = document.getElementById("sendBtn");
const newChatBtn = document.getElementById("newChatBtn");
const historyEl = document.getElementById("chatHistory");
const sidebarEl = document.getElementById("sidebar");
const toggleBtn = document.getElementById("sidebarToggle");

// ------------------------------------------------------------------
// Marked configuration
// ------------------------------------------------------------------
marked.setOptions({ breaks: true, gfm: true });

// ------------------------------------------------------------------
// SVG avatars (cloned from hidden templates in index.html)
// ------------------------------------------------------------------
function makeAvatar(role) {
  const wrap = document.createElement("div");
  wrap.className = "avatar";
  const tmplId = role === "user" ? "svg-user-avatar" : "svg-ai-avatar";
  const tmpl = document.getElementById(tmplId);
  if (tmpl) wrap.appendChild(tmpl.cloneNode(true));
  return wrap;
}

// ------------------------------------------------------------------
// State
// ------------------------------------------------------------------
let isWaiting = false;
let chatSessions = JSON.parse(localStorage.getItem("ragChatSessions") || "[]");
let currentSession = null;

// ------------------------------------------------------------------
// Input auto-resize + enable/disable send
// ------------------------------------------------------------------
inputEl.addEventListener("input", () => {
  inputEl.style.height = "auto";
  inputEl.style.height = Math.min(inputEl.scrollHeight, 200) + "px";
  sendBtn.disabled = inputEl.value.trim() === "";
});

inputEl.addEventListener("keydown", (e) => {
  if (e.key === "Enter" && !e.shiftKey) {
    e.preventDefault();
    if (!sendBtn.disabled && !isWaiting) sendMessage();
  }
});

sendBtn.addEventListener("click", () => { if (!isWaiting) sendMessage(); });

// ------------------------------------------------------------------
// Suggestion chips
// ------------------------------------------------------------------
document.querySelectorAll(".suggestion-chip").forEach((chip) => {
  chip.addEventListener("click", () => {
    inputEl.value = chip.dataset.query;
    inputEl.dispatchEvent(new Event("input"));
    sendMessage();
  });
});

// ------------------------------------------------------------------
// New chat / sidebar toggle
// ------------------------------------------------------------------
newChatBtn.addEventListener("click", resetChat);

toggleBtn.addEventListener("click", () => sidebarEl.classList.toggle("open"));
document.addEventListener("click", (e) => {
  if (sidebarEl.classList.contains("open") && !sidebarEl.contains(e.target) && e.target !== toggleBtn)
    sidebarEl.classList.remove("open");
});

// ------------------------------------------------------------------
// SEND MESSAGE  (SSE streaming)
// ------------------------------------------------------------------
async function sendMessage() {
  const text = inputEl.value.trim();
  if (!text || isWaiting) return;

  // Show chat area
  welcomeEl.style.display = "none";
  messagesEl.classList.add("visible");

  // Create session if needed
  if (!currentSession) {
    currentSession = {
      id: Date.now(),
      title: text.slice(0, 48) + (text.length > 48 ? "…" : ""),
      messages: [],
    };
    chatSessions.unshift(currentSession);
    saveSessions();
    renderHistory();
  }

  // User bubble
  appendMessage("user", text);
  currentSession.messages.push({ role: "user", content: text });

  // Reset input
  inputEl.value = "";
  inputEl.style.height = "auto";
  sendBtn.disabled = true;
  isWaiting = true;

  // Show typing indicator
  const typingEl = showTyping();

  // Create the assistant bubble that we'll stream into
  let streamBubble = null;
  let streamPara = null;
  let fullText = "";

  try {
    const resp = await fetch("/chat/stream", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ message: text }),
    });

    if (!resp.ok) {
      removeEl(typingEl);
      appendMessage("assistant", `**Error ${resp.status}**: Could not reach server.`);
      isWaiting = false;
      inputEl.focus();
      return;
    }

    // Remove typing indicator and create streaming bubble
    removeEl(typingEl);
    const { wrapper, bubble } = createStreamBubble();
    streamBubble = bubble;
    streamPara = document.createElement("p");
    bubble.appendChild(streamPara);
    // Blinking cursor
    const cursor = document.createElement("span");
    cursor.className = "streaming-cursor";
    bubble.appendChild(cursor);

    // Consume SSE
    const reader = resp.body.getReader();
    const decoder = new TextDecoder();
    let buffer = "";
    let sources = [];

    while (true) {
      const { done, value } = await reader.read();
      if (done) break;

      buffer += decoder.decode(value, { stream: true });
      const parts = buffer.split("\n\n");
      buffer = parts.pop(); // incomplete tail

      for (const part of parts) {
        if (!part.startsWith("data: ")) continue;
        let json;
        try { json = JSON.parse(part.slice(6)); } catch { continue; }

        if (json.error) {
          streamPara.textContent = `Error: ${json.error}`;
          cursor.remove();
          break;
        }

        if (json.chunk) {
          fullText += json.chunk;
          // Show raw text while streaming (fast, no re-render flicker)
          streamPara.textContent = fullText;
          scrollBottom();
        }

        if (json.done) {
          sources = json.sources || [];
        }
      }
    }

    // --- Streaming done: replace raw text with rendered markdown ---
    cursor.remove();
    streamBubble.innerHTML = marked.parse(fullText);
    processCodeBlocks(streamBubble);

    // Append sources
    if (sources.length > 0) {
      const srcDiv = document.createElement("div");
      srcDiv.className = "sources";
      sources.forEach((s) => {
        const chip = document.createElement("span");
        chip.className = "source-chip";
        // File icon via inline SVG
        chip.innerHTML = `<svg xmlns="http://www.w3.org/2000/svg" width="11" height="11" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M15 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V7Z"/><path d="M14 2v4a2 2 0 0 0 2 2h4"/></svg>`;
        chip.appendChild(document.createTextNode(" " + s));
        srcDiv.appendChild(chip);
      });
      streamBubble.appendChild(srcDiv);
    }

    // Re-init Lucide icons in new content
    try { lucide.createIcons(); } catch { }

    scrollBottom();

    // Save to session
    currentSession.messages.push({ role: "assistant", content: fullText });
    saveSessions();

  } catch (err) {
    removeEl(typingEl);
    if (streamBubble) {
      const cursor = streamBubble.querySelector(".streaming-cursor");
      if (cursor) cursor.remove();
      streamBubble.innerHTML = marked.parse(fullText || `**Connection error:** ${err.message}`);
    } else {
      appendMessage("assistant", `**Connection error:** ${err.message}`);
    }
  }

  isWaiting = false;
  inputEl.focus();
}

// ------------------------------------------------------------------
// Create a streaming assistant bubble (returns wrapper + bubble refs)
// ------------------------------------------------------------------
function createStreamBubble() {
  const wrapper = document.createElement("div");
  wrapper.className = "message assistant";

  const avatar = makeAvatar("assistant");
  const bubble = document.createElement("div");
  bubble.className = "bubble";

  wrapper.appendChild(avatar);
  wrapper.appendChild(bubble);
  messagesEl.appendChild(wrapper);

  return { wrapper, bubble };
}

// ------------------------------------------------------------------
// Append a complete message bubble (used for user msgs & history)
// ------------------------------------------------------------------
function appendMessage(role, content, sources = []) {
  const wrapper = document.createElement("div");
  wrapper.className = `message ${role}`;

  const avatar = makeAvatar(role);
  const bubble = document.createElement("div");
  bubble.className = "bubble";

  if (role === "assistant") {
    bubble.innerHTML = marked.parse(content);
    processCodeBlocks(bubble);
    if (sources.length > 0) {
      const srcDiv = document.createElement("div");
      srcDiv.className = "sources";
      sources.forEach((s) => {
        const chip = document.createElement("span");
        chip.className = "source-chip";
        chip.innerHTML = `<svg xmlns="http://www.w3.org/2000/svg" width="11" height="11" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M15 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V7Z"/><path d="M14 2v4a2 2 0 0 0 2 2h4"/></svg> ${s}`;
        srcDiv.appendChild(chip);
      });
      bubble.appendChild(srcDiv);
    }
  } else {
    bubble.textContent = content;
  }

  wrapper.appendChild(avatar);
  wrapper.appendChild(bubble);
  messagesEl.appendChild(wrapper);
  scrollBottom();
  return wrapper;
}

// ------------------------------------------------------------------
// Process code blocks: highlight + copy button
// ------------------------------------------------------------------
function processCodeBlocks(container) {
  container.querySelectorAll("pre").forEach((pre) => {
    const code = pre.querySelector("code");
    if (!code) return;

    const cls = Array.from(code.classList).find((c) => c.startsWith("language-"));
    const lang = cls ? cls.replace("language-", "") : "code";

    const header = document.createElement("div");
    header.className = "code-header";

    const langLabel = document.createElement("span");
    langLabel.className = "code-lang";
    langLabel.textContent = lang;

    const copyBtn = document.createElement("button");
    copyBtn.className = "copy-btn";
    copyBtn.innerHTML = `<svg xmlns="http://www.w3.org/2000/svg" width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect width="14" height="14" x="8" y="8" rx="2" ry="2"/><path d="M4 16c-1.1 0-2-.9-2-2V4c0-1.1.9-2 2-2h10c1.1 0 2 .9 2 2"/></svg> Copy`;

    copyBtn.addEventListener("click", () => {
      navigator.clipboard.writeText(code.innerText).then(() => {
        copyBtn.innerHTML = `<svg xmlns="http://www.w3.org/2000/svg" width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M20 6 9 17l-5-5"/></svg> Copied!`;
        copyBtn.classList.add("copied");
        setTimeout(() => {
          copyBtn.innerHTML = `<svg xmlns="http://www.w3.org/2000/svg" width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><rect width="14" height="14" x="8" y="8" rx="2" ry="2"/><path d="M4 16c-1.1 0-2-.9-2-2V4c0-1.1.9-2 2-2h10c1.1 0 2 .9 2 2"/></svg> Copy`;
          copyBtn.classList.remove("copied");
        }, 2000);
      });
    });

    header.appendChild(langLabel);
    header.appendChild(copyBtn);
    pre.insertBefore(header, code);
    hljs.highlightElement(code);
  });
}

// ------------------------------------------------------------------
// Typing indicator
// ------------------------------------------------------------------
function showTyping() {
  const wrapper = document.createElement("div");
  wrapper.className = "message assistant";

  const avatar = makeAvatar("assistant");
  const bubble = document.createElement("div");
  bubble.className = "bubble";
  bubble.innerHTML = `<div class="typing-indicator"><span></span><span></span><span></span></div>`;

  wrapper.appendChild(avatar);
  wrapper.appendChild(bubble);
  messagesEl.appendChild(wrapper);
  scrollBottom();
  return wrapper;
}

function removeEl(el) { if (el && el.parentNode) el.parentNode.removeChild(el); }
function scrollBottom() { messagesEl.scrollTo({ top: messagesEl.scrollHeight, behavior: "smooth" }); }

// ------------------------------------------------------------------
// Chat history persistence
// ------------------------------------------------------------------
function saveSessions() {
  const trimmed = chatSessions.slice(0, 20).map((s) => ({
    ...s,
    messages: s.messages.slice(-40),
  }));
  localStorage.setItem("ragChatSessions", JSON.stringify(trimmed));
}

function renderHistory() {
  historyEl.querySelectorAll(".history-item").forEach((el) => el.remove());
  chatSessions.forEach((session) => {
    const item = document.createElement("div");
    item.className = "history-item" + (session === currentSession ? " active" : "");
    item.textContent = session.title;
    item.addEventListener("click", () => loadSession(session));
    historyEl.appendChild(item);
  });
}

function loadSession(session) {
  currentSession = session;
  messagesEl.innerHTML = "";
  welcomeEl.style.display = "none";
  messagesEl.classList.add("visible");
  session.messages.forEach((m) => appendMessage(m.role, m.content));
  renderHistory();
}

function resetChat() {
  currentSession = null;
  messagesEl.innerHTML = "";
  messagesEl.classList.remove("visible");
  welcomeEl.style.display = "";
  inputEl.value = "";
  inputEl.style.height = "auto";
  sendBtn.disabled = true;
  renderHistory();
}

// ------------------------------------------------------------------
// Init
// ------------------------------------------------------------------
renderHistory();
inputEl.focus();
