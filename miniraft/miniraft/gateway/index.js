/**
 * Gateway Service
 * ───────────────
 * - Accepts browser WebSocket connections
 * - Discovers current RAFT leader by polling replicas
 * - Forwards strokes to the leader
 * - Broadcasts committed strokes to ALL connected clients
 * - Handles leader failover transparently (clients never disconnect)
 */

const express = require("express");
const http = require("http");
const WebSocket = require("ws");
const axios = require("axios");
const bodyParser = require("body-parser");

// ─── Config ───────────────────────────────────────────────────────────────────
const PORT = parseInt(process.env.GATEWAY_PORT || "3000");
const REPLICAS = (process.env.REPLICAS || "").split(",").filter(Boolean);

const LEADER_POLL_INTERVAL = 500;  // ms — how often we re-check who the leader is
const LEADER_TIMEOUT = 300;         // ms — HTTP timeout when checking a replica

// ─── State ────────────────────────────────────────────────────────────────────
let currentLeader = null;           // URL of the current leader replica
const clients = new Set();          // all connected WebSocket clients

// ─── Express + HTTP + WS setup ────────────────────────────────────────────────
const app = express();
app.use(bodyParser.json());
const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

function logInfo(msg) {
  console.log(`[Gateway] ${msg}`);
}

// ─── Leader Discovery ─────────────────────────────────────────────────────────

/**
 * Poll all replicas and return the URL of whichever one claims to be LEADER.
 * If none found, returns null.
 */
async function discoverLeader() {
  for (const replicaUrl of REPLICAS) {
    try {
      const res = await axios.get(`${replicaUrl}/status`, {
        timeout: LEADER_TIMEOUT,
      });
      if (res.data.state === "LEADER") {
        return replicaUrl;
      }
    } catch {
      /* replica unreachable */
    }
  }
  return null;
}

/** Keep currentLeader fresh on a background interval */
async function pollLeader() {
  const found = await discoverLeader();
  if (found && found !== currentLeader) {
    logInfo(`Leader changed → ${found}`);
    currentLeader = found;
  } else if (!found && currentLeader) {
    logInfo("No leader found — election in progress?");
    currentLeader = null;
  }
}

setInterval(pollLeader, LEADER_POLL_INTERVAL);
pollLeader(); // immediate first run

// ─── Broadcast to all WebSocket clients ───────────────────────────────────────
function broadcast(msg) {
  const data = JSON.stringify(msg);
  for (const client of clients) {
    if (client.readyState === WebSocket.OPEN) {
      client.send(data);
    }
  }
}

// ─── Forward stroke to leader with retry / failover ──────────────────────────
async function forwardStrokeToLeader(stroke, retries = 3) {
  for (let attempt = 0; attempt < retries; attempt++) {
    if (!currentLeader) {
      currentLeader = await discoverLeader();
    }
    if (!currentLeader) {
      logInfo("No leader available — waiting...");
      await new Promise((r) => setTimeout(r, 300));
      continue;
    }

    try {
      const res = await axios.post(
        `${currentLeader}/stroke`,
        { stroke },
        { timeout: 1000 }
      );
      if (res.data.ok) {
        logInfo(`Stroke committed at index ${res.data.index}`);
        // Broadcast the committed stroke to all clients
        broadcast({ type: "stroke", stroke });
        return;
      }
    } catch (err) {
      if (err.response?.status === 307) {
        // Replica redirected us — it knows who the leader is
        const hint = err.response.data?.leaderId;
        logInfo(`Redirected to leader hint: ${hint}`);
        // Find the matching replica URL
        currentLeader =
          REPLICAS.find((r) => r.includes(hint?.split(":")[0])) || null;
      } else {
        logInfo(`Leader ${currentLeader} unreachable — re-discovering...`);
        currentLeader = null;
      }
    }
  }
  logInfo("Failed to forward stroke after retries");
}

// ─── WebSocket Connection Handler ─────────────────────────────────────────────
wss.on("connection", async (ws) => {
  clients.add(ws);
  logInfo(`Client connected (total: ${clients.size})`);

  // Send full committed log so the new client can replay the canvas
  try {
    const leader = currentLeader || (await discoverLeader());
    if (leader) {
      const res = await axios.get(`${leader}/log`, { timeout: 500 });
      const entries = res.data.entries || [];
      for (const entry of entries) {
        ws.send(JSON.stringify({ type: "stroke", stroke: entry.stroke }));
      }
      logInfo(`Sent ${entries.length} historical strokes to new client`);
    }
  } catch {
    logInfo("Could not fetch log for new client");
  }

  ws.on("message", async (raw) => {
    let msg;
    try {
      msg = JSON.parse(raw);
    } catch {
      return;
    }

    if (msg.type === "stroke") {
      await forwardStrokeToLeader(msg.stroke);
    }
  });

  ws.on("close", () => {
    clients.delete(ws);
    logInfo(`Client disconnected (total: ${clients.size})`);
  });

  ws.on("error", (err) => {
    logInfo(`WS error: ${err.message}`);
    clients.delete(ws);
  });
});

// ─── REST Debug Endpoints ─────────────────────────────────────────────────────

/** GET /status — shows gateway state */
app.get("/status", (req, res) => {
  res.json({
    connectedClients: clients.size,
    currentLeader,
    replicas: REPLICAS,
  });
});

/** GET /cluster — shows all replica statuses */
app.get("/cluster", async (req, res) => {
  const results = await Promise.all(
    REPLICAS.map(async (r) => {
      try {
        const s = await axios.get(`${r}/status`, { timeout: 500 });
        return { url: r, ...s.data };
      } catch {
        return { url: r, state: "UNREACHABLE" };
      }
    })
  );
  res.json(results);
});

// ─── Start ────────────────────────────────────────────────────────────────────
server.listen(PORT, () => {
  logInfo(`Gateway listening on port ${PORT}`);
  logInfo(`Replicas: ${REPLICAS.join(", ")}`);
});

process.on("SIGTERM", () => {
  logInfo("SIGTERM — shutting down");
  process.exit(0);
});