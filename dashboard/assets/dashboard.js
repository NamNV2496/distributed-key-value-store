"use strict";

// The dashboard is deliberately dependency-free: it is served by the very node
// it inspects, so it must work inside a container with no internet access.

const REFRESH_MS = 2000;
const PALETTE = [
  "#4f9cf9", "#3fb950", "#f2b544", "#a371f7", "#f778ba",
  "#2dd4bf", "#fb923c", "#60a5fa", "#a3e635", "#f87171",
];

const el = (id) => document.getElementById(id);
const $ = {
  servedBy: el("served-by"),
  topoVersion: el("topo-version"),
  conn: el("conn"),
  updated: el("updated"),
  live: el("live"),
  refresh: el("refresh"),
  errorLine: el("error-line"),

  statShards: el("stat-shards"),
  statShardsNote: el("stat-shards-note"),
  statNodes: el("stat-nodes"),
  statNodesNote: el("stat-nodes-note"),
  statLeaders: el("stat-leaders"),
  statLeadersNote: el("stat-leaders-note"),
  statSlots: el("stat-slots"),
  statSlotsNote: el("stat-slots-note"),
  statMigrations: el("stat-migrations"),
  statMigrationsNote: el("stat-migrations-note"),

  routeForm: el("route-form"),
  routeKey: el("route-key"),
  samples: el("samples"),
  routeFlow: el("route-flow"),
  flowKey: el("flow-key"),
  flowTag: el("flow-tag"),
  flowSlot: el("flow-slot"),
  flowSlotRange: el("flow-slot-range"),
  flowShardStep: el("flow-shard-step"),
  flowShard: el("flow-shard"),
  flowShardSlots: el("flow-shard-slots"),
  flowLeader: el("flow-leader"),
  flowLeaderAddr: el("flow-leader-addr"),
  routeNote: el("route-note"),
  routeMembers: el("route-members"),

  slotbar: el("slotbar"),
  legend: el("legend"),
  shardGrid: el("shard-grid"),
  migrationsPanel: el("migrations-panel"),
  migrationsBody: el("migrations-body"),
  nodesBody: el("nodes-body"),
};

let status = null;      // last /cluster/status payload
let lastRoute = null;   // last /cluster/locate payload
let timer = null;

// ---------------------------------------------------------------- helpers

const num = (n) => (n === null || n === undefined ? "–" : n.toLocaleString());

function shardColor(shardID) {
  const ids = status ? status.shards.map((s) => s.id) : [];
  const index = ids.indexOf(shardID);
  if (index >= 0) return PALETTE[index % PALETTE.length];
  // Unknown shard (e.g. a migration target we have not rendered yet): derive a
  // stable colour from the name so it does not flicker between refreshes.
  let hash = 0;
  for (const ch of shardID) hash = (hash * 31 + ch.charCodeAt(0)) >>> 0;
  return PALETTE[hash % PALETTE.length];
}

// Roles arrive capitalised from the manager ("Leader") and lower-case from the
// per-shard endpoint ("leader"); normalise before comparing.
const roleOf = (raw) => (raw || "").toLowerCase();

function setText(node, value) {
  node.textContent = value === "" || value === undefined || value === null ? " " : String(value);
}

function element(tag, className, text) {
  const node = document.createElement(tag);
  if (className) node.className = className;
  if (text !== undefined) node.textContent = text;
  return node;
}

// ---------------------------------------------------------------- fetching

async function getJSON(url) {
  const res = await fetch(url, { headers: { Accept: "application/json" }, cache: "no-store" });
  if (!res.ok) {
    const body = (await res.text()).trim();
    throw new Error(body || `${res.status} ${res.statusText}`);
  }
  return res.json();
}

async function load() {
  try {
    status = await getJSON("/cluster/status");
    $.conn.dataset.state = "ok";
    $.conn.textContent = "connected";
    $.errorLine.textContent = "";
    $.updated.textContent = new Date(status.generated_at).toLocaleTimeString();
    render();
  } catch (err) {
    $.conn.dataset.state = "error";
    $.conn.textContent = "unreachable";
    $.errorLine.textContent = err.message;
  }
}

// ---------------------------------------------------------------- rendering

function render() {
  renderStats();
  renderSlotBar();
  renderShards();
  renderMigrations();
  renderNodes();
  if (lastRoute) renderRoute(lastRoute); // keep the traced route in sync
}

function renderStats() {
  const shards = status.shards;
  const nodes = status.nodes;
  const online = nodes.filter((n) => n.reachable).length;
  const withLeader = shards.filter((s) => s.leader_id).length;
  const slots = shards.map((s) => s.slots);
  const spread = slots.length ? Math.max(...slots) - Math.min(...slots) : 0;

  // A standalone dashboard names the node it read the topology from; a node
  // serving its own dashboard just names itself.
  setText($.servedBy, status.source ? `${status.served_by} via ${status.source}` : status.served_by);
  setText($.topoVersion, "v" + status.topology.version);

  setText($.statShards, num(shards.length));
  setText($.statShardsNote, shards.length ? shards.map((s) => s.id).join(", ") : "no shards");

  setText($.statNodes, num(nodes.length));
  $.statNodesNote.className = "stat-note " + (online === nodes.length ? "ok" : "bad");
  setText($.statNodesNote, `${online} reachable, ${nodes.length - online} down`);

  setText($.statLeaders, `${withLeader}/${shards.length}`);
  $.statLeadersNote.className = "stat-note " + (withLeader === shards.length ? "ok" : "warn");
  setText($.statLeadersNote, withLeader === shards.length ? "every shard has a leader" : "election in progress");

  setText($.statSlots, num(status.topology.slot_count));
  setText($.statSlotsNote, shards.length ? `spread ${spread} slot${spread === 1 ? "" : "s"} across shards` : "");

  const migrations = status.migrations || [];
  setText($.statMigrations, num(migrations.length));
  $.statMigrationsNote.className = "stat-note " + (migrations.length ? "warn" : "");
  setText($.statMigrationsNote, migrations.length ? "writes deferred on those slots" : "cluster is stable");
}

function renderSlotBar() {
  $.slotbar.replaceChildren();
  $.legend.replaceChildren();

  const total = status.topology.slot_count || 1;
  for (const shard of status.shards) {
    const color = shardColor(shard.id);
    const pct = (shard.slots / total) * 100;

    const seg = element("div", "slotbar-seg");
    seg.style.width = pct + "%";
    seg.style.background = color;
    seg.title = `${shard.id}: ${shard.slots} slots (${pct.toFixed(1)}%)`;
    if (pct > 9) seg.textContent = shard.id;
    $.slotbar.appendChild(seg);

    const item = element("div", "legend-item");
    const dot = element("span", "legend-dot");
    dot.style.background = color;
    item.append(dot, element("b", null, shard.id),
      element("span", null, `${shard.slots} slots · ${pct.toFixed(1)}%`));
    $.legend.appendChild(item);
  }
}

function renderShards() {
  $.shardGrid.replaceChildren();
  if (!status.shards.length) {
    $.shardGrid.appendChild(element("p", "empty", "This node knows of no shard."));
    return;
  }

  for (const shard of status.shards) {
    const card = element("article", "shard-card");
    card.style.setProperty("--shard-color", shardColor(shard.id));
    if (lastRoute && lastRoute.shard === shard.id) card.classList.add("is-target");

    const top = element("div", "shard-top");
    top.append(element("span", "shard-id", shard.id),
      element("span", "shard-slots", `${num(shard.slots)} slots · ${(shard.share * 100).toFixed(1)}%`));

    const meta = element("div", "shard-meta");
    const quorum = shard.members.length > 0 && shard.healthy_members > shard.members.length / 2;
    meta.append(
      element("span", "chip " + (shard.leader_id ? "ok" : "warn"),
        shard.leader_id ? `leader ${shard.leader_id}` : "no leader"),
      element("span", "chip", `term ${shard.term}`),
      element("span", "chip " + (quorum ? "" : "bad"),
        `${shard.healthy_members}/${shard.members.length} up${quorum ? "" : " · no quorum"}`),
    );

    const list = element("ul", "members");
    for (const member of shard.members) {
      const isLeader = member.reachable && shard.leader_id === member.node_id;
      const role = !member.reachable ? "offline" : (isLeader ? "leader" : roleOf(member.role) || "follower");

      const row = element("li", "member");
      row.dataset.role = role;
      row.dataset.reachable = String(member.reachable);

      const mark = element("span", "member-mark", role === "leader" ? "★" : role === "offline" ? "✕" : "○");
      mark.title = role;

      const who = element("div");
      who.append(element("div", "member-id", member.node_id + (member.local ? " (this node)" : "")),
        element("div", "member-addr", member.address));

      const right = element("div", "member-right");
      right.append(element("div", "member-role", role),
        element("div", "member-term",
          member.reachable ? `term ${member.term} · idx ${member.commit_index}` : "unreachable"));

      row.append(mark, who, right);
      if (member.error) row.title = member.error;
      list.appendChild(row);
    }

    card.append(top, meta, list);
    $.shardGrid.appendChild(card);
  }
}

function renderMigrations() {
  const migrations = status.migrations || [];
  $.migrationsPanel.hidden = migrations.length === 0;
  $.migrationsBody.replaceChildren();
  for (const m of migrations) {
    const row = element("tr");
    row.append(element("td", "mono", m.slot), element("td", "mono", m.from),
      element("td", "mono", m.to), element("td", null, m.state));
    $.migrationsBody.appendChild(row);
  }
}

function renderNodes() {
  $.nodesBody.replaceChildren();
  for (const node of status.nodes) {
    const row = element("tr");

    // The probe error can be a full dial trace; keep it in the tooltip so one
    // dead node cannot stretch the table.
    const health = element("td");
    health.append(element("span", "dot " + (node.reachable ? "up" : "down")),
      document.createTextNode(node.reachable ? "up" : "unreachable"));
    if (node.error) health.title = node.error;

    const hostedShards = node.shards || [];
    const hosted = hostedShards.length
      ? hostedShards.map((s) => `${s.shard_id} (${roleOf(s.role) || "?"})`).join(", ")
      : "—";

    row.append(
      element("td", "mono", node.node_id + (node.local ? " ★" : "")),
      element("td", "mono", node.address),
      health,
      element("td", "mono", hosted),
      element("td", "mono", node.reachable ? "v" + node.topology_version : "—"),
      element("td", "num mono", node.local ? "local" : node.latency_ms + " ms"),
    );
    $.nodesBody.appendChild(row);
  }
}

// ---------------------------------------------------------------- routing

async function traceRoute(key) {
  if (!key) return;
  try {
    lastRoute = await getJSON("/cluster/locate?key=" + encodeURIComponent(key));
    $.errorLine.textContent = "";
    renderRoute(lastRoute);
    renderShards(); // re-highlight the shard card that owns this key
  } catch (err) {
    lastRoute = null;
    $.routeFlow.hidden = false;
    $.routeNote.hidden = false;
    $.routeNote.dataset.kind = "error";
    $.routeNote.textContent = "Could not locate that key: " + err.message;
  }
}

function renderRoute(route) {
  $.routeFlow.hidden = false;

  setText($.flowKey, route.key);
  setText($.flowTag, route.hash_tag && route.hash_tag !== route.key
    ? `hash tag {${route.hash_tag}}`
    : "hashed whole");

  setText($.flowSlot, num(route.slot));
  setText($.flowSlotRange, `of ${num(route.slot_count)} slots`);

  const shard = status && status.shards.find((s) => s.id === route.shard);
  setText($.flowShard, route.shard || "unassigned");
  $.flowShardStep.dataset.accent = "true";
  $.flowShardStep.style.setProperty("--step-color", route.shard ? shardColor(route.shard) : "var(--line)");
  setText($.flowShardSlots, shard ? `owns ${num(shard.slots)} slots` : "");

  const leaderID = shard && shard.leader_id;
  const leader = shard && shard.members.find((m) => m.node_id === leaderID);
  setText($.flowLeader, leaderID || "electing…");
  setText($.flowLeaderAddr, leader ? leader.address : "no leader yet — writes wait for the election");

  // Members of the owning shard, leader first: this is the set of processes
  // that will hold the key once the write commits.
  $.routeMembers.replaceChildren();
  const nodes = route.nodes || {};
  const memberIDs = Object.keys(nodes).sort((a, b) =>
    (a === leaderID ? -1 : b === leaderID ? 1 : a.localeCompare(b)));

  for (const nodeID of memberIDs) {
    const member = shard && shard.members.find((m) => m.node_id === nodeID);
    const reachable = !member || member.reachable;
    const isLeader = nodeID === leaderID && reachable;

    const pill = element("div", "route-member");
    pill.dataset.role = !reachable ? "offline" : (isLeader ? "leader" : "follower");
    pill.append(
      element("span", null, !reachable ? "✕" : isLeader ? "★" : "○"),
      element("span", "who", nodeID),
      element("span", "addr", nodes[nodeID]),
      element("span", "addr", !reachable ? "offline" : isLeader ? "executes + replicates" : "replicates"),
    );
    $.routeMembers.appendChild(pill);
  }

  if (route.migrating) {
    $.routeNote.hidden = false;
    $.routeNote.dataset.kind = "warn";
    $.routeNote.textContent =
      `Slot ${route.migrating.slot} is migrating from ${route.migrating.from} to ${route.migrating.to} ` +
      `(${route.migrating.state}). Writes to this key are rejected with a retry hint until it lands.`;
  } else if (!route.shard) {
    $.routeNote.hidden = false;
    $.routeNote.dataset.kind = "error";
    $.routeNote.textContent = "No shard owns this slot — the topology is incomplete.";
  } else {
    $.routeNote.hidden = true;
  }
}

// ---------------------------------------------------------------- wiring

const SAMPLES = ["user:1000", "user:1001", "{cart}:items", "session:abc", "leaderboard"];
for (const sample of SAMPLES) {
  const button = element("button", "sample", sample);
  button.type = "button";
  button.addEventListener("click", () => {
    $.routeKey.value = sample;
    traceRoute(sample);
  });
  $.samples.appendChild(button);
}

$.routeForm.addEventListener("submit", (event) => {
  event.preventDefault();
  traceRoute($.routeKey.value.trim());
});

function schedule() {
  clearInterval(timer);
  if ($.live.checked) timer = setInterval(load, REFRESH_MS);
}

$.live.addEventListener("change", schedule);
$.refresh.addEventListener("click", load);
document.addEventListener("visibilitychange", () => {
  // Polling a hidden tab keeps every node busy for nobody's benefit.
  if (document.hidden) clearInterval(timer);
  else { load(); schedule(); }
});

load().then(() => traceRoute($.routeKey.value.trim()));
schedule();
