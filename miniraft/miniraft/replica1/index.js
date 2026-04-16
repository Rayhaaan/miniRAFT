/**
 * Mini-RAFT Replica Node
 * ──────────────────────
 * States: FOLLOWER → CANDIDATE → LEADER
 *
 * Key timers:
 *   - Election timeout : 500–800 ms random (reset on every heartbeat received)
 *   - Heartbeat interval: 150 ms (only active when this node is LEADER)
 */

const express = require("express");
const axios = require("axios");
const bodyParser = require("body-parser");

// ─── Config from environment ──────────────────────────────────────────────────
const REPLICA_ID = process.env.REPLICA_ID || "replica1";
const PORT = parseInt(process.env.REPLICA_PORT || "4001");
const PEERS = (process.env.PEERS || "").split(",").filter(Boolean);

const HEARTBEAT_INTERVAL_MS = 150;
const ELECTION_TIMEOUT_MIN = 500;
const ELECTION_TIMEOUT_MAX = 800;

// ─── Node State ───────────────────────────────────────────────────────────────
let state = "FOLLOWER";   // FOLLOWER | CANDIDATE | LEADER
let currentTerm = 0;
let votedFor = null;       // which candidateId we voted for in currentTerm
let leaderId = null;       // known leader this term

// Append-only stroke log: [ { index, term, stroke } ]
let log = [];
let commitIndex = -1;      // highest log index known to be committed

let electionTimer = null;
let heartbeatTimer = null;

const app = express();
app.use(bodyParser.json());

// ─── Helpers ──────────────────────────────────────────────────────────────────

function randomElectionTimeout() {
  return Math.floor(
    Math.random() * (ELECTION_TIMEOUT_MAX - ELECTION_TIMEOUT_MIN) +
      ELECTION_TIMEOUT_MIN
  );
}

function logInfo(msg) {
  console.log(`[${REPLICA_ID}][Term ${currentTerm}][${state}] ${msg}`);
}

/** Reset election timer — called on every heartbeat or valid AppendEntries */
function resetElectionTimer() {
  if (electionTimer) clearTimeout(electionTimer);
  electionTimer = setTimeout(startElection, randomElectionTimeout());
}

/** Transition to FOLLOWER; optionally update term */
function becomeFollower(term, newLeader = null) {
  if (term > currentTerm) {
    currentTerm = term;
    votedFor = null;
  }
  state = "FOLLOWER";
  leaderId = newLeader;
  stopHeartbeat();
  resetElectionTimer();
  logInfo(`→ FOLLOWER${newLeader ? ` (leader: ${newLeader})` : ""}`);
}

/** Start a new election */
async function startElection() {
  state = "CANDIDATE";
  currentTerm += 1;
  votedFor = REPLICA_ID;
  leaderId = null;
  logInfo("→ CANDIDATE — starting election");

  let votes = 1; // vote for self
  const majority = Math.floor((PEERS.length + 1) / 2) + 1;

  const voteRequests = PEERS.map(async (peer) => {
    try {
      const res = await axios.post(
        `${peer}/request-vote`,
        {
          term: currentTerm,
          candidateId: REPLICA_ID,
          lastLogIndex: log.length - 1,
          lastLogTerm: log.length > 0 ? log[log.length - 1].term : -1,
        },
        { timeout: 300 }
      );
      if (res.data.voteGranted) {
        votes += 1;
        logInfo(`Got vote from ${peer} (total: ${votes})`);
      } else if (res.data.term > currentTerm) {
        // Discovered higher term
        becomeFollower(res.data.term);
      }
    } catch {
      logInfo(`No response from ${peer} during election`);
    }
  });

  await Promise.all(voteRequests);

  if (state !== "CANDIDATE") return; // something changed during voting

  if (votes >= majority) {
    becomeLeader();
  } else {
    logInfo("Election lost — reverting to FOLLOWER");
    becomeFollower(currentTerm);
  }
}

/** Transition to LEADER */
function becomeLeader() {
  state = "LEADER";
  leaderId = REPLICA_ID;
  logInfo("→ LEADER 🎉");
  stopElectionTimer();
  startHeartbeat();
}

function stopElectionTimer() {
  if (electionTimer) clearTimeout(electionTimer);
  electionTimer = null;
}

/** Broadcast heartbeats to all followers every 150 ms */
function startHeartbeat() {
  heartbeatTimer = setInterval(async () => {
    for (const peer of PEERS) {
      try {
        await axios.post(
          `${peer}/heartbeat`,
          { term: currentTerm, leaderId: REPLICA_ID },
          { timeout: 200 }
        );
      } catch {
        /* peer down — ignore */
      }
    }
  }, HEARTBEAT_INTERVAL_MS);
}

function stopHeartbeat() {
  if (heartbeatTimer) clearInterval(heartbeatTimer);
  heartbeatTimer = null;
}

// ─── RPC Endpoints ────────────────────────────────────────────────────────────

/**
 * POST /request-vote
 * Body: { term, candidateId, lastLogIndex, lastLogTerm }
 */
app.post("/request-vote", (req, res) => {
  const { term, candidateId, lastLogIndex, lastLogTerm } = req.body;

  // Outdated candidate
  if (term < currentTerm) {
    return res.json({ term: currentTerm, voteGranted: false });
  }

  if (term > currentTerm) {
    becomeFollower(term);
  }

  // Check if we can vote for this candidate
  const alreadyVoted = votedFor !== null && votedFor !== candidateId;
  const myLastIndex = log.length - 1;
  const myLastTerm = log.length > 0 ? log[log.length - 1].term : -1;

  const candidateLogUpToDate =
    lastLogTerm > myLastTerm ||
    (lastLogTerm === myLastTerm && lastLogIndex >= myLastIndex);

  if (!alreadyVoted && candidateLogUpToDate) {
    votedFor = candidateId;
    resetElectionTimer();
    logInfo(`Voted for ${candidateId}`);
    return res.json({ term: currentTerm, voteGranted: true });
  }

  return res.json({ term: currentTerm, voteGranted: false });
});

/**
 * POST /heartbeat
 * Body: { term, leaderId }
 */
app.post("/heartbeat", (req, res) => {
  const { term, leaderId: newLeader } = req.body;

  if (term < currentTerm) {
    return res.json({ term: currentTerm, success: false });
  }

  becomeFollower(term, newLeader);
  return res.json({ term: currentTerm, success: true });
});

/**
 * POST /append-entries
 * Body: { term, leaderId, prevLogIndex, prevLogTerm, entry }
 * Used to replicate a single stroke entry to followers.
 */
app.post("/append-entries", (req, res) => {
  const { term, leaderId: newLeader, prevLogIndex, prevLogTerm, entry } = req.body;

  if (term < currentTerm) {
    return res.json({ term: currentTerm, success: false, logLength: log.length });
  }

  becomeFollower(term, newLeader);

  // Consistency check
  if (prevLogIndex >= 0) {
    if (log.length <= prevLogIndex || log[prevLogIndex].term !== prevLogTerm) {
      logInfo(`Log inconsistency at index ${prevLogIndex} — sending log length ${log.length}`);
      return res.json({ term: currentTerm, success: false, logLength: log.length });
    }
  }

  // Append entry
  if (entry) {
    // Truncate any conflicting entries
    log = log.slice(0, prevLogIndex + 1);
    log.push(entry);
    logInfo(`Appended entry at index ${entry.index} (stroke from client)`);
  }

  return res.json({ term: currentTerm, success: true, logLength: log.length });
});

/**
 * POST /sync-log
 * Body: { fromIndex } — leader sends all committed entries from fromIndex onward
 * Called on a restarted follower to catch it up quickly.
 */
app.post("/sync-log", (req, res) => {
  const { fromIndex } = req.body;
  const missing = log.slice(fromIndex).filter((e) => e.index <= commitIndex);
  logInfo(`Sync-log request from index ${fromIndex} — sending ${missing.length} entries`);
  res.json({ entries: missing, commitIndex });
});

/**
 * POST /commit
 * Internal: leader tells followers to advance their commitIndex.
 * Body: { commitIndex: N }
 */
app.post("/commit", (req, res) => {
  const { commitIndex: newCommit } = req.body;
  if (newCommit > commitIndex) {
    commitIndex = newCommit;
    logInfo(`Committed up to index ${commitIndex}`);
  }
  res.json({ ok: true });
});

/**
 * POST /stroke
 * Called by the Gateway to submit a new drawing stroke.
 * Only the LEADER processes this; followers reject with a redirect hint.
 */
app.post("/stroke", async (req, res) => {
  if (state !== "LEADER") {
    return res.status(307).json({ error: "not-leader", leaderId });
  }

  const stroke = req.body.stroke;
  const newIndex = log.length;
  const entry = { index: newIndex, term: currentTerm, stroke };

  // 1. Append to own log
  log.push(entry);
  logInfo(`Appended stroke at index ${newIndex}`);

  // 2. Replicate to followers
  let acks = 1; // self
  const majority = Math.floor((PEERS.length + 1) / 2) + 1;

  const replicaPromises = PEERS.map(async (peer) => {
    try {
      const r = await axios.post(
        `${peer}/append-entries`,
        {
          term: currentTerm,
          leaderId: REPLICA_ID,
          prevLogIndex: newIndex - 1,
          prevLogTerm: newIndex > 0 ? log[newIndex - 1]?.term ?? -1 : -1,
          entry,
        },
        { timeout: 300 }
      );
      if (r.data.success) {
        acks += 1;
      } else if (r.data.logLength < newIndex) {
        // Follower is behind — trigger sync
        try {
          await axios.post(
            `${peer}/sync-log-push`,
            { entries: log.slice(r.data.logLength), commitIndex },
            { timeout: 500 }
          );
        } catch { /* best-effort */ }
      }
    } catch {
      logInfo(`Failed to replicate to ${peer}`);
    }
  });

  await Promise.all(replicaPromises);

  if (acks >= majority) {
    commitIndex = newIndex;
    logInfo(`✅ Committed stroke at index ${newIndex} (acks: ${acks})`);

    // Notify followers to advance commit index
    PEERS.forEach((peer) => {
      axios
        .post(`${peer}/commit`, { commitIndex }, { timeout: 200 })
        .catch(() => {});
    });

    return res.json({ ok: true, index: newIndex });
  } else {
    logInfo(`❌ Stroke at index ${newIndex} failed quorum (acks: ${acks})`);
    return res.status(500).json({ error: "quorum-not-reached" });
  }
});

/**
 * POST /sync-log-push
 * Leader pushes missing entries directly to a lagging follower.
 */
app.post("/sync-log-push", (req, res) => {
  const { entries, commitIndex: leaderCommit } = req.body;
  for (const entry of entries) {
    if (!log[entry.index]) {
      log.push(entry);
    }
  }
  if (leaderCommit > commitIndex) commitIndex = leaderCommit;
  logInfo(`Sync-push: now have ${log.length} entries, commitIndex=${commitIndex}`);
  res.json({ ok: true });
});

/**
 * GET /status
 * Debug endpoint — shows current node state.
 */
app.get("/status", (req, res) => {
  res.json({
    replicaId: REPLICA_ID,
    state,
    currentTerm,
    leaderId,
    logLength: log.length,
    commitIndex,
  });
});

/**
 * GET /log
 * Returns committed strokes for canvas replay.
 */
app.get("/log", (req, res) => {
  res.json({ entries: log.filter((e) => e.index <= commitIndex) });
});

// ─── Bootstrap ────────────────────────────────────────────────────────────────
app.listen(PORT, () => {
  logInfo(`Replica listening on port ${PORT}`);
  logInfo(`Peers: ${PEERS.join(", ")}`);
  becomeFollower(0);
});

// Graceful shutdown — let Docker's SIGTERM trigger a clean exit
process.on("SIGTERM", () => {
  logInfo("SIGTERM — shutting down gracefully");
  stopHeartbeat();
  stopElectionTimer();
  process.exit(0);
});