var socket = io();
var chatDiv = document.getElementById("chat");
var form = document.getElementById("chatForm");
var input = document.getElementById("msgInput");
var loadingIndicator = document.getElementById("loadingIndicator");

var isLoadingMessages = false;
var hasMoreMessages = true;
var autoScrollEnabled = true;
var oldestTimestamp = null;
var newestTimestamp = null;
var PAGE_LIMIT = 20;
var INITIAL_LIMIT = 50;
var isInitialLoad = true;
var messageHashes = new Set(); // Track message hashes to prevent duplicates

form.onsubmit = function (e) {
  e.preventDefault();
  const msg = input.value.trim();
  if (msg && socket.connected) {
    socket.emit("send_message", msg);
    input.value = "";
  }
};

// Scroll event handler for loading older messages
chatDiv.addEventListener("scroll", function () {
  const nearTop = chatDiv.scrollTop <= 50;

  if (
    nearTop &&
    hasMoreMessages &&
    !isLoadingMessages &&
    oldestTimestamp &&
    !isInitialLoad
  ) {
    console.log("Loading older messages, current oldest:", oldestTimestamp);
    loadOlderMessages();
  }

  // Check if user is near bottom to enable auto-scroll
  const threshold = 100;
  const isNearBottom =
    chatDiv.scrollHeight - chatDiv.scrollTop - chatDiv.clientHeight < threshold;
  autoScrollEnabled = isNearBottom;
});

function loadOlderMessages() {
  if (isLoadingMessages || !hasMoreMessages || !oldestTimestamp) {
    console.log("Cannot load older messages:", {
      isLoadingMessages,
      hasMoreMessages,
      oldestTimestamp,
    });
    return;
  }

  isLoadingMessages = true;
  loadingIndicator.style.display = "block";

  console.log("Emitting load_older_messages with timestamp:", oldestTimestamp);
  socket.emit("load_older_messages", {
    before_timestamp: oldestTimestamp,
    limit: PAGE_LIMIT,
  });
}

function createMessageElement(msg) {
  const msgId = msg.hash || msg.sender + msg.timestamp + msg.message;

  if (messageHashes.has(msgId)) {
    return null;
  }
  messageHashes.add(msgId);

  const div = document.createElement("div");

  const isOwnMessage =
    msg.sender === window.CLIENT_NAME || msg.sender === "you";

  // Apply bubble style based on sender
  if (isOwnMessage) {
    div.className = "msg you";
  } else {
    div.className = "msg peer";
  }

  div.setAttribute("data-timestamp", msg.timestamp || "");
  div.setAttribute("data-msg-id", msgId);

  const msgText = document.createElement("div");
  msgText.className = "msg-text";

  if (isOwnMessage) {
    msgText.textContent = "You: " + msg.message;
  } else {
    msgText.textContent = msg.sender + ": " + msg.message;
  }

  const timeDate = document.createElement("div");
  timeDate.className = "time-date";
  const displayTime = msg.display_timestamp || msg.timestamp;
  timeDate.textContent = formatTimestamp(
    displayTime || new Date().toISOString()
  );

  div.appendChild(msgText);
  div.appendChild(timeDate);
  return div;
}

function formatTimestamp(timestampStr) {
  try {
    let ts;
    if (!timestampStr) {
      ts = new Date();
    } else if (timestampStr.includes("T") || timestampStr.includes("Z")) {
      ts = new Date(timestampStr);
    } else {
      // Handle the format from the server
      ts = new Date(timestampStr.replace(" ", "T"));
    }
    if (isNaN(ts.getTime())) ts = new Date();
    const dateStr = ts.toLocaleDateString([], {
      month: "short",
      day: "numeric",
    });
    const timeStr = ts.toLocaleTimeString([], {
      hour: "2-digit",
      minute: "2-digit",
      hour12: false,
    });
    return `${dateStr} ${timeStr}`;
  } catch (e) {
    const now = new Date();
    return `${now.toLocaleDateString([], {
      month: "short",
      day: "numeric",
    })} ${now.toLocaleTimeString([], {
      hour: "2-digit",
      minute: "2-digit",
      hour12: false,
    })}`;
  }
}

function updateTimestampBounds(messages) {
  if (!messages || messages.length === 0) return;

  // Update oldest timestamp (first message in chronological array)
  const firstMsg = messages[0];
  if (firstMsg && firstMsg.timestamp) {
    if (!oldestTimestamp || firstMsg.timestamp < oldestTimestamp) {
      oldestTimestamp = firstMsg.timestamp;
    }
  }

  // Update newest timestamp (last message in chronological array)
  const lastMsg = messages[messages.length - 1];
  if (lastMsg && lastMsg.timestamp) {
    if (!newestTimestamp || lastMsg.timestamp > newestTimestamp) {
      newestTimestamp = lastMsg.timestamp;
    }
  }

  console.log(
    "Updated timestamps - oldest:",
    oldestTimestamp,
    "newest:",
    newestTimestamp
  );
}

// When a new live message arrives
socket.on("receive_message", function (data) {
  console.log("Received live message:", data);
  const messageElement = createMessageElement(data);

  if (messageElement) {
    chatDiv.appendChild(messageElement);

    // Update newest timestamp for live messages
    if (
      data.timestamp &&
      (!newestTimestamp || data.timestamp > newestTimestamp)
    ) {
      newestTimestamp = data.timestamp;
    }

    // Auto-scroll to bottom only when the user is at/near bottom
    if (autoScrollEnabled) {
      setTimeout(() => {
        chatDiv.scrollTop = chatDiv.scrollHeight;
      }, 10);
    }
  }
});

// Initial load of latest messages
socket.on("chat_history", function (messages) {
  console.log("Received chat history:", messages.length, "messages");

  // Clear existing messages (except loading indicator) and message hashes
  const existingMessages = chatDiv.querySelectorAll(".msg");
  existingMessages.forEach((msg) => msg.remove());
  messageHashes.clear();

  if (!messages || messages.length === 0) {
    oldestTimestamp = null;
    newestTimestamp = null;
    hasMoreMessages = false;
    isInitialLoad = false;
    setTimeout(() => {
      chatDiv.scrollTop = chatDiv.scrollHeight;
    }, 50);
    return;
  }

  // Append messages in chronological order (oldest -> newest)
  messages.forEach(function (msg) {
    const el = createMessageElement(msg);
    if (el) {
      chatDiv.appendChild(el);
    }
  });

  // Set timestamp boundaries
  updateTimestampBounds(messages);

  // If we got fewer messages than requested, no more history available
  hasMoreMessages = messages.length >= INITIAL_LIMIT;
  isInitialLoad = false;

  console.log("Initial load complete. hasMoreMessages:", hasMoreMessages);

  // Scroll to bottom on initial load
  setTimeout(() => {
    chatDiv.scrollTop = chatDiv.scrollHeight;
    autoScrollEnabled = true;
  }, 100);
});

// Handler for older messages (pagination)
socket.on("older_messages", function (messages) {
  console.log("Received older messages:", messages.length, "messages");
  loadingIndicator.style.display = "none";

  if (!messages || messages.length === 0) {
    hasMoreMessages = false;
    isLoadingMessages = false;
    console.log("No more older messages available");
    return;
  }

  // Preserve current scroll position
  const prevScrollHeight = chatDiv.scrollHeight;
  const prevScrollTop = chatDiv.scrollTop;

  // Prepend messages in chronological order (oldest -> newest)
  // We need to insert them BEFORE the current first message (after loading indicator)
  const firstMessageElement = chatDiv.querySelector(".msg");

  messages.forEach(function (msg) {
    const el = createMessageElement(msg);
    if (el && firstMessageElement) {
      chatDiv.insertBefore(el, firstMessageElement);
    } else if (el) {
      chatDiv.appendChild(el);
    }
  });

  // Update oldest timestamp with the new oldest message
  if (messages.length > 0) {
    const newOldest = messages[0].timestamp;
    if (newOldest && (!oldestTimestamp || newOldest < oldestTimestamp)) {
      oldestTimestamp = newOldest;
    }
  }

  // Restore scroll position
  const newScrollHeight = chatDiv.scrollHeight;
  chatDiv.scrollTop = prevScrollTop + (newScrollHeight - prevScrollHeight);

  // If we got fewer messages than requested, we've reached the beginning
  if (messages.length < PAGE_LIMIT) {
    hasMoreMessages = false;
    console.log("Reached the beginning of chat history");
  }

  isLoadingMessages = false;
  console.log("Older messages loaded. New oldest timestamp:", oldestTimestamp);
});

function refreshChat() {
  console.log("Refreshing chat with initial limit:", INITIAL_LIMIT);
  socket.emit("refresh_chat", { limit: INITIAL_LIMIT });
}

// Auto-refresh every 30 seconds to ensure sync
setInterval(function () {
  if (socket.connected) {
    socket.emit("check_new_messages");
  }
}, 30000);

// Load initial messages when page opens
socket.on("connect", function () {
  refreshChat();
});

// Handle sync status updates
socket.on("sync_status", function (data) {
  console.log("Sync status:", data.status);
});
