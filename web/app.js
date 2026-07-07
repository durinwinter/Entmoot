const state = {
  id: "ent-1",
  mqtt_listen: "0.0.0.0:1883",
  bus_listen: ["tcp/0.0.0.0:7447"],
  peers: [],
  discovery_mode: "named",
  scope: "entmoot",
  max_packet_size: 262144,
  max_connections: 10000,
  max_publish_rate: 500,
  data_dir: "",
  metrics_listen: "",
  session_expiry_secs: 86400,
  max_queued_per_session: 1000,
  tls_enabled: false,
  tls_listen: "0.0.0.0:8883",
  cert_file: "",
  key_file: "",
  client_ca_file: "",
  allow_anonymous: false,
  default_policy: "deny",
  topology: {
    nodes: [
      { id: "ent-1", mqtt: "0.0.0.0:1883", max_clients: 10000 },
      { id: "ent-2", mqtt: "0.0.0.0:1884", max_clients: 10000 },
      { id: "ent-3", mqtt: "0.0.0.0:1885", max_clients: 10000 },
    ],
    clientGroups: [
      { name: "PLC gateways", count: 120, nodeId: "ent-1", topic: "plant/#" },
      { name: "SCADA stations", count: 18, nodeId: "ent-2", topic: "plant/#, cmd/#" },
      { name: "Historians", count: 8, nodeId: "ent-3", topic: "plant/#" },
      { name: "Edge sensors", count: 420, nodeId: "ent-1", topic: "plant/+/telemetry" },
    ],
  },
  users: [
    {
      name: "plc1",
      password_sha256: "5db1fee4b5703808c48078a76768b155b421b210c0761cd6a5d223f4d99f1eaa",
    },
    {
      name: "scada",
      password_sha256: "5db1fee4b5703808c48078a76768b155b421b210c0761cd6a5d223f4d99f1eaa",
    },
  ],
  acl: [
    { user: "plc1", publish: "plant/#", subscribe: "cmd/plc1/#" },
    { user: "scada", publish: "cmd/#", subscribe: "plant/#" },
  ],
};

const saved = window.localStorage.getItem("entmoot-console");
let savedHadTopology = false;
if (saved) {
  try {
    const savedState = JSON.parse(saved);
    savedHadTopology = Boolean(savedState.topology);
    Object.assign(state, savedState);
  } catch (_error) {
    window.localStorage.removeItem("entmoot-console");
  }
}

normalizeState(savedHadTopology);

const $ = (selector) => document.querySelector(selector);
const $$ = (selector) => Array.from(document.querySelectorAll(selector));
const svgNS = "http://www.w3.org/2000/svg";
const artboardWidth = 2730;
const artboardHeight = 1536;
const columnRepeatStep = 671 / artboardHeight;
const columnRepeatOverlap = 30;
const leftColumnRepeatExtraOverlap = 14;
const leftColumnRepeatScaleY = 1.026;
const shellChromeY = 48;
const maxColumnRepeats = 6;
let shellRepeatCount = -1;

function normalizeState(savedHadTopologyDraft) {
  const legacyBusKey = "zen" + "oh_listen";
  if (!Array.isArray(state.bus_listen)) {
    state.bus_listen = Array.isArray(state[legacyBusKey]) ? state[legacyBusKey] : ["tcp/0.0.0.0:7447"];
  }
  delete state[legacyBusKey];
  if (!state.topology || !Array.isArray(state.topology.nodes) || !Array.isArray(state.topology.clientGroups)) {
    state.topology = { nodes: [], clientGroups: [] };
  }
  if (!state.topology.nodes.length) {
    state.topology.nodes.push({
      id: state.id || "ent-1",
      mqtt: state.mqtt_listen || "0.0.0.0:1883",
      max_clients: Number(state.max_connections) || 10000,
    });
  }
  const primary = state.topology.nodes[0];
  if (!savedHadTopologyDraft) {
    primary.id = state.id || primary.id || "ent-1";
    primary.mqtt = state.mqtt_listen || primary.mqtt || "0.0.0.0:1883";
    primary.max_clients = Number(state.max_connections) || Number(primary.max_clients) || 10000;
  }
  primary.id = primary.id || state.id || "ent-1";
  primary.mqtt = primary.mqtt || state.mqtt_listen || "0.0.0.0:1883";
  primary.max_clients = Number(primary.max_clients) || Number(state.max_connections) || 10000;
  state.id = state.id || primary.id;
  state.mqtt_listen = state.mqtt_listen || primary.mqtt;
  state.max_connections = Number(state.max_connections) || primary.max_clients;
  state.topology.clientGroups.forEach((group) => {
    group.name = group.name || "Client group";
    group.count = Number(group.count) || 0;
    group.nodeId = group.nodeId || primary.id;
    group.topic = group.topic || "#";
  });
}

function ensureShellRepeats(repeatCount) {
  if (repeatCount === shellRepeatCount) return;

  const shell = $(".ent-shell");
  const bottomLeft = $(".frame-bottom-left");
  if (!shell || !bottomLeft) return;

  $$(".frame-repeat-left, .frame-repeat-right").forEach((node) => node.remove());

  for (let index = 0; index < repeatCount; index += 1) {
    const left = document.createElement("img");
    left.className = "frame frame-repeat-left";
    left.src = "./assets/ent-shell/left_column.webp";
    left.alt = "";
    left.setAttribute("aria-hidden", "true");

    const right = document.createElement("img");
    right.className = "frame frame-repeat-right";
    right.src = "./assets/ent-shell/right_column.webp";
    right.alt = "";
    right.setAttribute("aria-hidden", "true");

    shell.insertBefore(left, bottomLeft);
    shell.insertBefore(right, bottomLeft);
  }

  shellRepeatCount = repeatCount;
}

function measureEntShell() {
  const shell = $(".ent-shell");
  if (!shell) return;

  const shellWidth = shell.getBoundingClientRect().width;
  const baseHeight = shellWidth > 0 ? (shellWidth * artboardHeight) / artboardWidth : 840;
  const repeatStepPx = baseHeight * columnRepeatStep;
  const repeatStridePx = Math.max(1, repeatStepPx - columnRepeatOverlap);
  const viewportTarget = window.innerHeight - shellChromeY;
  const targetHeight = Math.max(840, viewportTarget, baseHeight);
  const repeatCount = Math.min(
    maxColumnRepeats,
    Math.max(0, Math.ceil((targetHeight - baseHeight) / repeatStridePx)),
  );
  const totalHeight = baseHeight + repeatStridePx * repeatCount;
  const compact = shellWidth < 760;
  const headerTop = compact ? Math.max(76, baseHeight * 0.24) : baseHeight * 0.244;
  const wellTop = compact ? Math.max(128, baseHeight * 0.42) : baseHeight * 0.315;
  const contentTop = compact ? Math.max(138, baseHeight * 0.46) : baseHeight * 0.335;
  const wellBottom = compact ? 58 : baseHeight * 0.094;
  const contentBottom = compact ? 72 : baseHeight * 0.115;

  shell.style.setProperty("--shell-base-height", `${baseHeight}px`);
  shell.style.setProperty("--shell-height", `${totalHeight}px`);
  shell.style.setProperty("--header-top", `${headerTop}px`);
  shell.style.setProperty("--well-top", `${wellTop}px`);
  shell.style.setProperty("--well-bottom", `${wellBottom}px`);
  shell.style.setProperty("--content-top", `${contentTop}px`);
  shell.style.setProperty("--content-bottom", `${contentBottom}px`);

  ensureShellRepeats(repeatCount);

  const repeats = $$(".frame-repeat-left, .frame-repeat-right");
  repeats.forEach((frame, index) => {
    const repeatIndex = Math.floor(index / 2) + 1;
    const isLeft = frame.classList.contains("frame-repeat-left");
    const overlap = isLeft ? columnRepeatOverlap + leftColumnRepeatExtraOverlap : columnRepeatOverlap;
    const top = repeatStepPx * repeatIndex - overlap * repeatIndex;
    frame.style.setProperty("--repeat-top", `${top}px`);
    frame.style.transform = isLeft
      ? `translateY(${2 + repeatIndex - 1}px) scaleY(${leftColumnRepeatScaleY})`
      : `translateY(${2 + repeatIndex - 1}px)`;
    frame.style.transformOrigin = "top center";
  });
}

function quote(value) {
  return `"${String(value).replaceAll("\\", "\\\\").replaceAll('"', '\\"')}"`;
}

function list(values) {
  const parts = values.map((value) => value.trim()).filter(Boolean).map(quote);
  return `[${parts.join(", ")}]`;
}

function scalarLine(key, value) {
  if (typeof value === "number") return `${key} = ${value}`;
  return `${key} = ${quote(value)}`;
}

function csv(value) {
  return String(value)
    .split(",")
    .map((item) => item.trim())
    .filter(Boolean);
}

function syncPrimaryNode(previousId) {
  const primary = state.topology.nodes[0];
  if (!primary) return;

  if (previousId && previousId !== state.id) {
    state.topology.clientGroups.forEach((group) => {
      if (group.nodeId === previousId) group.nodeId = state.id;
    });
  }
  primary.id = state.id;
  primary.mqtt = state.mqtt_listen;
  primary.max_clients = Number(state.max_connections) || 0;
}

function syncStaticField(key, value) {
  const field = $(`[data-field='${key}']`);
  if (field) field.value = value;
}

function generateToml() {
  const lines = [
    scalarLine("id", state.id),
    scalarLine("mqtt_listen", state.mqtt_listen),
    `bus_listen = ${list(state.bus_listen)}`,
    `peers = ${state.discovery_mode === "isolated" ? "[]" : list(state.peers)}`,
    scalarLine("scope", state.scope),
    scalarLine("max_packet_size", Number(state.max_packet_size)),
    scalarLine("max_connections", Number(state.max_connections)),
    scalarLine("max_publish_rate", Number(state.max_publish_rate)),
  ];

  if (state.data_dir.trim()) lines.push(scalarLine("data_dir", state.data_dir));
  if (state.metrics_listen.trim()) lines.push(scalarLine("metrics_listen", state.metrics_listen));

  lines.push(
    scalarLine("session_expiry_secs", Number(state.session_expiry_secs)),
    scalarLine("max_queued_per_session", Number(state.max_queued_per_session)),
  );

  if (state.tls_enabled) {
    lines.push(
      "",
      "[tls]",
      scalarLine("listen", state.tls_listen),
      scalarLine("cert_file", state.cert_file || "/etc/entmoot/server.pem"),
      scalarLine("key_file", state.key_file || "/etc/entmoot/server.key"),
    );
    if (state.client_ca_file.trim()) {
      lines.push(scalarLine("client_ca_file", state.client_ca_file));
    }
  }

  lines.push(
    "",
    "[auth]",
    `allow_anonymous = ${state.allow_anonymous === true || state.allow_anonymous === "true"}`,
    scalarLine("default_policy", state.default_policy),
  );

  state.users.forEach((user) => {
    if (!user.name.trim()) return;
    lines.push(
      "",
      "[[auth.users]]",
      scalarLine("name", user.name),
      scalarLine("password_sha256", user.password_sha256),
    );
  });

  state.acl.forEach((rule) => {
    if (!rule.user.trim()) return;
    lines.push(
      "",
      "[[acl]]",
      scalarLine("user", rule.user),
      `publish = ${list(csv(rule.publish))}`,
      `subscribe = ${list(csv(rule.subscribe))}`,
    );
  });

  return `${lines.join("\n")}\n`;
}

function bindStaticFields() {
  $$("[data-field]").forEach((field) => {
    const key = field.dataset.field;
    if (field.type === "checkbox") {
      field.checked = Boolean(state[key]);
    } else if (state[key] !== undefined) {
      field.value = state[key];
    }
    field.addEventListener("input", () => {
      const previous = state[key];
      if (field.type === "number") state[key] = Number(field.value);
      else if (field.type === "checkbox") state[key] = field.checked;
      else state[key] = field.value;
      if (key === "id" || key === "mqtt_listen" || key === "max_connections") {
        syncPrimaryNode(key === "id" ? previous : undefined);
      }
      render();
    });
  });

  $$("[data-list]").forEach((field) => {
    const key = field.dataset.list;
    field.value = state[key][0] || "";
    field.addEventListener("input", () => {
      state[key] = [field.value].filter(Boolean);
      render();
    });
  });

  $("#tlsEnabled").checked = state.tls_enabled;
  $("#tlsEnabled").addEventListener("change", (event) => {
    state.tls_enabled = event.target.checked;
    render();
  });
}

function updateOutputs() {
  renderSummary();
  renderTopologyGraph();
  updateCapacityReadouts();
  $("#tomlOutput").textContent = generateToml();
  window.localStorage.setItem("entmoot-console", JSON.stringify(state));
}

function rowInput(label, value, onInput, type = "text") {
  const wrapper = document.createElement("label");
  wrapper.textContent = label;
  const input = document.createElement("input");
  input.type = type;
  input.value = value;
  input.autocomplete = "off";
  input.addEventListener("input", () => onInput(input.value));
  input.addEventListener("change", () => render());
  wrapper.append(input);
  return wrapper;
}

function removeButton(onClick) {
  const button = document.createElement("button");
  button.type = "button";
  button.className = "remove";
  button.textContent = "x";
  button.setAttribute("aria-label", "Remove row");
  button.addEventListener("click", onClick);
  return button;
}

function svgEl(name, attributes = {}) {
  const node = document.createElementNS(svgNS, name);
  Object.entries(attributes).forEach(([key, value]) => node.setAttribute(key, String(value)));
  return node;
}

function topologyMetrics() {
  const nodes = state.topology.nodes;
  const groups = state.topology.clientGroups;
  const loadByNode = Object.fromEntries(nodes.map((node) => [node.id, 0]));
  groups.forEach((group) => {
    loadByNode[group.nodeId] = (loadByNode[group.nodeId] || 0) + Number(group.count || 0);
  });
  const totalClients = groups.reduce((sum, group) => sum + Number(group.count || 0), 0);
  const totalCapacity = nodes.reduce((sum, node) => sum + Number(node.max_clients || 0), 0);
  return { nodes, groups, loadByNode, totalClients, totalCapacity };
}

function renderTopologyGraph() {
  const svg = $("#topologyGraph");
  if (!svg || !state.topology) return;
  const { nodes, groups, loadByNode, totalClients, totalCapacity } = topologyMetrics();
  svg.replaceChildren();

  const defs = svgEl("defs");
  const glow = svgEl("filter", { id: "graphGlow", x: "-30%", y: "-30%", width: "160%", height: "160%" });
  glow.append(
    svgEl("feGaussianBlur", { stdDeviation: "2.5", result: "blur" }),
    svgEl("feMerge"),
  );
  glow.lastChild.append(svgEl("feMergeNode", { in: "blur" }), svgEl("feMergeNode", { in: "SourceGraphic" }));
  defs.append(glow);
  svg.append(defs);

  const nodePositions = nodes.map((node, index) => ({
    node,
    x: 635,
    y: 72 + index * Math.max(68, 230 / Math.max(1, nodes.length - 1)),
  }));
  const groupPositions = groups.map((group, index) => ({
    group,
    x: 150,
    y: 56 + index * Math.max(50, 250 / Math.max(1, groups.length - 1)),
  }));

  groupPositions.forEach(({ group, x, y }, index) => {
    const target = nodePositions.find((entry) => entry.node.id === group.nodeId) || nodePositions[0];
    if (!target) return;
    const load = loadByNode[target.node.id] || 0;
    const overloaded = load > Number(target.node.max_clients || 0);
    const width = Math.max(1.5, Math.min(7, Number(group.count || 0) / 80 + 1.5));
    const path = svgEl("path", {
      d: `M ${x + 68} ${y} C 310 ${y}, 420 ${target.y}, ${target.x - 76} ${target.y}`,
      class: overloaded ? "graph-edge warn" : "graph-edge",
      "stroke-width": width,
    });
    svg.append(path);

    const cluster = svgEl("g", { class: "graph-client", transform: `translate(${x}, ${y})` });
    const radius = Math.max(16, Math.min(28, 14 + Math.sqrt(Number(group.count || 0))));
    cluster.append(
      svgEl("circle", { r: radius, cx: 0, cy: 0 }),
      svgEl("text", { x: 42, y: -6, class: "graph-title" }),
      svgEl("text", { x: 42, y: 13, class: "graph-subtitle" }),
    );
    cluster.children[1].textContent = group.name || `Clients ${index + 1}`;
    cluster.children[2].textContent = `${Number(group.count || 0).toLocaleString()} clients`;
    svg.append(cluster);
  });

  nodePositions.forEach(({ node, x, y }) => {
    const load = loadByNode[node.id] || 0;
    const max = Number(node.max_clients || 0);
    const ratio = max > 0 ? Math.min(1, load / max) : 1;
    const overloaded = load > max;
    const broker = svgEl("g", { class: overloaded ? "graph-node overloaded" : "graph-node", transform: `translate(${x}, ${y - 34})` });
    broker.append(
      svgEl("rect", { width: 176, height: 68, rx: 8 }),
      svgEl("text", { x: 16, y: 24, class: "graph-title" }),
      svgEl("text", { x: 16, y: 45, class: "graph-subtitle" }),
      svgEl("rect", { x: 16, y: 54, width: 144, height: 5, rx: 3, class: "graph-meter-bg" }),
      svgEl("rect", { x: 16, y: 54, width: Math.max(3, 144 * ratio), height: 5, rx: 3, class: "graph-meter-fill" }),
    );
    broker.children[1].textContent = node.id;
    broker.children[2].textContent = `${load.toLocaleString()} / ${max.toLocaleString()} clients`;
    svg.append(broker);
  });

  const footer = svgEl("text", { x: 22, y: 338, class: "graph-footer" });
  footer.textContent = `${totalClients.toLocaleString()} planned clients across ${nodes.length} broker nodes, ${totalCapacity.toLocaleString()} total capacity`;
  svg.append(footer);
}

function updateCapacityReadouts() {
  if (!$("#nodeCapacity")) return;
  const { loadByNode } = topologyMetrics();
  $$("#nodeCapacity .topology-row").forEach((row, index) => {
    const node = state.topology.nodes[index];
    if (!node) return;
    const load = loadByNode[node.id] || 0;
    const max = Number(node.max_clients || 0);
    row.classList.toggle("overloaded", load > max);
    const readout = row.querySelector(".capacity-readout");
    if (readout) {
      readout.innerHTML = `<strong>${load.toLocaleString()}</strong><span>${max.toLocaleString()} max</span>`;
    }
  });
}

function renderNodeCapacity() {
  const root = $("#nodeCapacity");
  root.replaceChildren();
  const { loadByNode } = topologyMetrics();

  state.topology.nodes.forEach((node, index) => {
    const load = loadByNode[node.id] || 0;
    const max = Number(node.max_clients || 0);
    const overloaded = load > max;
    const row = document.createElement("div");
    row.className = overloaded ? "topology-row overloaded" : "topology-row";

    const idInput = rowInput("Node ID", node.id, (value) => {
      const previous = node.id;
      node.id = value || previous;
      state.topology.clientGroups.forEach((group) => {
        if (group.nodeId === previous) group.nodeId = node.id;
      });
      if (index === 0) {
        state.id = node.id;
        syncStaticField("id", node.id);
      }
      updateOutputs();
    });

    const mqttInput = rowInput("MQTT", node.mqtt, (value) => {
      node.mqtt = value;
      if (index === 0) {
        state.mqtt_listen = value;
        syncStaticField("mqtt_listen", value);
      }
      updateOutputs();
    });

    const maxInput = rowInput("Max clients", max, (value) => {
      node.max_clients = Number(value) || 0;
      if (index === 0) {
        state.max_connections = node.max_clients;
        syncStaticField("max_connections", node.max_clients);
      }
      updateOutputs();
    }, "number");

    const capacity = document.createElement("div");
    capacity.className = "capacity-readout";
    capacity.innerHTML = `<strong>${load.toLocaleString()}</strong><span>${max.toLocaleString()} max</span>`;

    row.append(idInput, mqttInput, maxInput, capacity);
    if (index > 0) {
      row.append(removeButton(() => {
        state.topology.nodes.splice(index, 1);
        state.topology.clientGroups.forEach((group) => {
          if (group.nodeId === node.id) group.nodeId = state.topology.nodes[0].id;
        });
        render();
      }));
    }
    root.append(row);
  });
}

function renderClientGroups() {
  const root = $("#clientGroups");
  root.replaceChildren();

  state.topology.clientGroups.forEach((group, index) => {
    const row = document.createElement("div");
    row.className = "topology-row client-row";
    const target = document.createElement("label");
    target.textContent = "Broker node";
    const select = document.createElement("select");
    state.topology.nodes.forEach((node) => {
      const option = document.createElement("option");
      option.value = node.id;
      option.textContent = node.id;
      select.append(option);
    });
    select.value = group.nodeId;
    select.addEventListener("input", () => {
      group.nodeId = select.value;
      updateOutputs();
    });
    target.append(select);

    row.append(
      rowInput("Group", group.name, (value) => {
        group.name = value;
        updateOutputs();
      }),
      rowInput("Clients", group.count, (value) => {
        group.count = Number(value) || 0;
        updateOutputs();
      }, "number"),
      target,
      rowInput("Topics", group.topic, (value) => {
        group.topic = value;
        updateOutputs();
      }),
      removeButton(() => {
        state.topology.clientGroups.splice(index, 1);
        render();
      }),
    );
    root.append(row);
  });
}

function renderTopology() {
  renderTopologyGraph();
  renderNodeCapacity();
  renderClientGroups();
}

function renderPeers() {
  const root = $("#peers");
  root.replaceChildren();
  if (state.discovery_mode === "isolated") return;

  state.peers.forEach((peer, index) => {
    const row = document.createElement("div");
    row.className = "row peer-row";
    row.append(
      rowInput("Peer endpoint", peer, (value) => {
        state.peers[index] = value;
        updateOutputs();
      }),
      removeButton(() => {
        state.peers.splice(index, 1);
        render();
      }),
    );
    root.append(row);
  });
}

function renderUsers() {
  const root = $("#users");
  root.replaceChildren();
  state.users.forEach((user, index) => {
    const row = document.createElement("div");
    row.className = "row user-row";
    row.append(
      rowInput("Name", user.name, (value) => {
        state.users[index].name = value;
        updateOutputs();
      }),
      rowInput("Password SHA-256", user.password_sha256, (value) => {
        state.users[index].password_sha256 = value;
        updateOutputs();
      }),
      removeButton(() => {
        state.users.splice(index, 1);
        render();
      }),
    );
    root.append(row);
  });
}

function renderAcl() {
  const root = $("#aclRules");
  root.replaceChildren();
  state.acl.forEach((rule, index) => {
    const row = document.createElement("div");
    row.className = "row";
    row.append(
      rowInput("Identity", rule.user, (value) => {
        state.acl[index].user = value;
        updateOutputs();
      }),
      rowInput("Publish filters", rule.publish, (value) => {
        state.acl[index].publish = value;
        updateOutputs();
      }),
      rowInput("Subscribe filters", rule.subscribe, (value) => {
        state.acl[index].subscribe = value;
        updateOutputs();
      }),
      removeButton(() => {
        state.acl.splice(index, 1);
        render();
      }),
    );
    root.append(row);
  });
}

function renderSummary() {
  const { nodes, totalClients, totalCapacity } = topologyMetrics();
  $("#nodeCount").textContent = String(nodes.length);
  $("#peerCount").textContent = state.discovery_mode === "isolated" ? "0" : String(Math.max(state.peers.filter(Boolean).length, nodes.length - 1));
  $("#connectionCap").textContent = `${totalClients.toLocaleString()} / ${totalCapacity.toLocaleString()}`;
  $("#topologyTotals").textContent = `${totalClients.toLocaleString()} planned clients`;
  const anonymous = state.allow_anonymous === true || state.allow_anonymous === "true";
  $("#securityMode").textContent = anonymous ? "Open" : state.tls_enabled ? "TLS" : "ACL";
}

function render() {
  $("[data-field='discovery_mode']").value = state.discovery_mode;
  $("#tlsEnabled").checked = state.tls_enabled;
  $$(".tls-field").forEach((field) => field.classList.toggle("hidden", !state.tls_enabled));
  renderTopology();
  renderPeers();
  renderUsers();
  renderAcl();
  updateOutputs();
}

$("#addPeerBtn").addEventListener("click", () => {
  state.discovery_mode = "named";
  state.peers.push("tcp/10.0.0.2:7447");
  render();
});

$("#addUserBtn").addEventListener("click", () => {
  state.users.push({ name: "operator", password_sha256: "" });
  render();
});

$("#addAclBtn").addEventListener("click", () => {
  state.acl.push({ user: "*", publish: "plant/#", subscribe: "plant/#" });
  render();
});

$("#addTopologyNodeBtn").addEventListener("click", () => {
  const next = state.topology.nodes.length + 1;
  state.topology.nodes.push({
    id: `ent-${next}`,
    mqtt: `0.0.0.0:${1882 + next}`,
    max_clients: Number(state.max_connections) || 10000,
  });
  render();
});

$("#addClientGroupBtn").addEventListener("click", () => {
  state.topology.clientGroups.push({
    name: "New clients",
    count: 50,
    nodeId: state.topology.nodes[0].id,
    topic: "plant/#",
  });
  render();
});

$("#resetBtn").addEventListener("click", () => {
  window.localStorage.removeItem("entmoot-console");
  window.location.reload();
});

$("#copyBtn").addEventListener("click", async () => {
  await navigator.clipboard.writeText(generateToml());
  $("#copyState").textContent = "Copied";
  window.setTimeout(() => {
    $("#copyState").textContent = "Ready";
  }, 1500);
});

bindStaticFields();
render();
measureEntShell();
window.addEventListener("resize", measureEntShell);
