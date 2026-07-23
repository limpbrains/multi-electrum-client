// Interactive demo: one ElectrumManager, three local electrum servers
// (via ws↔tcp bridges from the docker `demo` profile), live-visualized
// routing. See ../../docker/compose.yml and vite.config.ts for the
// plumbing.

import {
  ElectrumManager,
  failover,
  preferFastest,
  roundRobin,
  type ClientView,
  type Outcome,
  type RoutingPolicy,
  type ServerSpec,
} from 'multi-electrum-client';

// --- static config ---------------------------------------------------------

interface DemoServer {
  id: string;
  port: number;
  color: string;
  /** Toxiproxy proxy name for the demo lane. */
  proxy: string;
}

const DEMO_SERVERS: readonly DemoServer[] = [
  { id: 'electrumx', port: 52002, color: '#4f9cf9', proxy: 'electrumx-demo' },
  { id: 'fulcrum', port: 52102, color: '#f9a84f', proxy: 'fulcrum-demo' },
  { id: 'electrs', port: 52202, color: '#5fd68a', proxy: 'electrs-demo' },
];

const spec = (s: DemoServer): ServerSpec => ({
  id: s.id,
  host: '127.0.0.1',
  port: s.port,
  protocol: 'ws',
});

// Any 64-hex scripthash works — unknown hashes return an empty balance.
const DEMO_SCRIPTHASH = '8b01df4e368ea28f8dc0423bcf7a4923e3a12d307c875e47a0cfbf90b5c39161';

// --- tiny DOM helpers ------------------------------------------------------

const $ = <T extends HTMLElement>(sel: string): T => {
  const el = document.querySelector<T>(sel);
  if (!el) throw new Error(`missing element ${sel}`);
  return el;
};

const logEl = $('#log');
function log(text: string, cls = ''): void {
  const row = document.createElement('div');
  row.className = 'row';
  const t = new Date().toLocaleTimeString();
  row.innerHTML = `<span class="t">${t}</span><span class="${cls}"></span>`;
  (row.lastElementChild as HTMLElement).textContent = text;
  logEl.prepend(row);
  while (logEl.childElementCount > 200) logEl.lastElementChild?.remove();
}

// --- switchable policy -----------------------------------------------------

const policies: Record<string, () => RoutingPolicy> = {
  roundRobin: () => roundRobin(),
  preferFastest: () => preferFastest({ withinPct: 20 }),
  failover: () => failover(['electrumx', 'fulcrum', 'electrs']),
};

let inner: RoutingPolicy = policies['roundRobin']!();

const policy: RoutingPolicy = {
  pick: (ctx) => inner.pick(ctx),
  onOutcome: (o) => {
    handleOutcome(o);
    inner.onOutcome?.(o);
  },
};

// --- manager ---------------------------------------------------------------

const manager = new ElectrumManager({
  network: 'regtest',
  servers: DEMO_SERVERS.map(spec),
  policy,
  // Individual routing decisions per request read better on screen than
  // coalesced wire batches.
  autoBatch: false,
  requestTimeoutMs: 8000,
  // Tighter than the default 30s cap so a restored lane visibly comes
  // back within a few seconds.
  reconnectBackoff: { minMs: 500, maxMs: 5000, factor: 2, jitter: 0.2 },
});

manager.on('client-state', ({ clientId, state }) => {
  log(`${clientId}: ${state}`, state === 'connected' ? 'good' : '');
  refreshCards();
});
manager.on('client-banned', ({ clientId, until }) => {
  log(`${clientId}: BANNED for ${Math.round((until - Date.now()) / 1000)}s`, 'err');
});
manager.on('subscription-restored', ({ method, drift }) => {
  log(`subscription restored: ${method}${drift ? ' (drifted — caught up)' : ''}`, 'good');
});
manager.on('error', (e) => {
  log(`error: ${(e as Error).message ?? e}`, 'err');
});

// --- server cards ----------------------------------------------------------

interface CardRefs {
  root: HTMLElement;
  state: HTMLElement;
  fields: Record<string, HTMLElement>;
  toggle: HTMLButtonElement;
  cut: HTMLButtonElement;
}

const cards = new Map<string, CardRefs>();
const removed = new Set<string>();
const cardsWrap = $('#cards');
const template = $<HTMLTemplateElement>('#card-template');

for (const s of DEMO_SERVERS) {
  const node = (template.content.cloneNode(true) as DocumentFragment).firstElementChild!;
  const root = node as HTMLElement;
  root.dataset['id'] = s.id;
  root.querySelector<HTMLElement>('.dot')!.style.background = s.color;
  root.querySelector('.name')!.textContent = `${s.id} :${s.port}`;
  const fields: Record<string, HTMLElement> = {};
  for (const el of Array.from(root.querySelectorAll<HTMLElement>('.stats [data-k]'))) {
    fields[el.dataset['k']!] = el;
  }
  const refs: CardRefs = {
    root,
    state: root.querySelector('.state')!,
    fields,
    toggle: root.querySelector('[data-k="toggle"]')!,
    cut: root.querySelector('[data-k="cut"]')!,
  };
  cards.set(s.id, refs);
  cardsWrap.appendChild(root);

  // Remove / re-add the server through the library API.
  refs.toggle.addEventListener('click', () => {
    if (removed.has(s.id)) {
      removed.delete(s.id);
      manager.addServer(spec(s));
      refs.toggle.textContent = 'disconnect';
      log(`addServer(${s.id})`);
    } else {
      removed.add(s.id);
      void manager.removeServer(s.id).then(() => refreshCards());
      refs.toggle.textContent = 'reconnect';
      log(`removeServer(${s.id})`);
    }
  });

  // Cut / restore the toxiproxy lane (transport-level fault).
  let lanesCut = false;
  refs.cut.addEventListener('click', () => {
    lanesCut = !lanesCut;
    refs.cut.textContent = lanesCut ? 'restore lane' : 'cut lane';
    refs.cut.classList.toggle('active', lanesCut);
    void toxiproxy(`/proxies/${s.proxy}`, 'POST', { enabled: !lanesCut });
    log(`${s.id}: lane ${lanesCut ? 'CUT' : 'restored'}`, lanesCut ? 'err' : 'good');
  });

  // Latency toxic slider.
  const lag = root.querySelector<HTMLInputElement>('[data-k="lag"]')!;
  const lagLabel = root.querySelector<HTMLElement>('[data-k="lag-label"]')!;
  lag.addEventListener('change', () => {
    const ms = Number(lag.value);
    lagLabel.textContent = String(ms);
    void setLatencyToxic(s.proxy, ms);
    log(`${s.id}: +${ms}ms lane latency`);
  });
  lag.addEventListener('input', () => {
    lagLabel.textContent = lag.value;
  });
}

async function toxiproxy(path: string, method: string, body?: unknown): Promise<Response> {
  return fetch(`/toxiproxy${path}`, {
    method,
    ...(body !== undefined ? { body: JSON.stringify(body) } : {}),
  });
}

async function setLatencyToxic(proxy: string, ms: number): Promise<void> {
  await toxiproxy(`/proxies/${proxy}/toxics/demo-latency`, 'DELETE').catch(() => undefined);
  if (ms > 0) {
    await toxiproxy(`/proxies/${proxy}/toxics`, 'POST', {
      name: 'demo-latency',
      type: 'latency',
      stream: 'downstream',
      attributes: { latency: ms, jitter: Math.round(ms / 5) },
    });
  }
}

function refreshCards(): void {
  const views = new Map(manager.getClientViews().map((v) => [v.id, v]));
  for (const s of DEMO_SERVERS) {
    const refs = cards.get(s.id)!;
    const v: ClientView | undefined = views.get(s.id);
    const state = removed.has(s.id) ? 'removed' : (v?.state ?? 'disconnected');
    refs.state.textContent = state;
    refs.state.className = `state badge ${state}`;
    refs.root.className = `card state-${state}`;
    if (!v) continue;
    const { latency, errors, success, inFlight } = v.telemetry;
    refs.fields['ema']!.textContent = `ema ${latency.ema.toFixed(0)}ms`;
    refs.fields['p50']!.textContent = `p50 ${latency.p50.toFixed(0)}`;
    refs.fields['p95']!.textContent = `p95 ${latency.p95.toFixed(0)}`;
    refs.fields['ok']!.textContent = `ok ${success.count}`;
    refs.fields['err']!.textContent = `err ${(errors.rate * 100).toFixed(0)}%`;
    refs.fields['inflight']!.textContent = `inflight ${inFlight}`;
  }
}

setInterval(refreshCards, 500);

// --- packet animation (SVG) ------------------------------------------------

interface Packet {
  serverId: string;
  color: string;
  start: number;
  duration: number;
}

const packets: Packet[] = [];
const svg = $('#lanes') as unknown as SVGSVGElement;
const viz = $('#viz');

function laneEndpoints(serverId: string): { x1: number; y1: number; x2: number; y2: number } {
  const vizBox = viz.getBoundingClientRect();
  const browserBox = $('#browser-node').getBoundingClientRect();
  const cardBox = cards.get(serverId)!.root.getBoundingClientRect();
  return {
    x1: browserBox.right - vizBox.left,
    y1: browserBox.top + browserBox.height / 2 - vizBox.top,
    x2: cardBox.left - vizBox.left,
    y2: cardBox.top + cardBox.height / 2 - vizBox.top,
  };
}

function drawFrame(): void {
  const now = performance.now();
  let content = '';
  // Static lanes.
  for (const s of DEMO_SERVERS) {
    const { x1, y1, x2, y2 } = laneEndpoints(s.id);
    const cut = removed.has(s.id);
    content += `<line x1="${x1}" y1="${y1}" x2="${x2}" y2="${y2}" stroke="${cut ? '#3a2430' : '#26314f'}" stroke-width="2" ${cut ? 'stroke-dasharray="4 6"' : ''}/>`;
  }
  // Flying packets: out on the first half, back on the second.
  for (let i = packets.length - 1; i >= 0; i--) {
    const p = packets[i]!;
    const t = (now - p.start) / p.duration;
    if (t >= 1) {
      packets.splice(i, 1);
      continue;
    }
    const { x1, y1, x2, y2 } = laneEndpoints(p.serverId);
    const phase = t < 0.5 ? t * 2 : (1 - t) * 2; // 0→1→0
    const x = x1 + (x2 - x1) * phase;
    const y = y1 + (y2 - y1) * phase;
    content += `<circle cx="${x}" cy="${y}" r="5" fill="${p.color}" opacity="0.9"/>`;
  }
  svg.innerHTML = content;
  requestAnimationFrame(drawFrame);
}
requestAnimationFrame(drawFrame);

// --- outcome feed: animation + chart + distribution ------------------------

interface ChartPoint {
  t: number;
  latency: number;
}

const chartData = new Map<string, ChartPoint[]>(DEMO_SERVERS.map((s) => [s.id, []]));

function handleOutcome(o: Outcome): void {
  if (o.kind !== 'success' && o.kind !== 'error') return;
  const server = DEMO_SERVERS.find((s) => s.id === o.clientId);
  if (!server) return;
  let color = server.color;
  if (o.kind === 'error') {
    const retryable = o.error === 'transport' || o.error === 'timeout' || o.error === 'rate-limit';
    color = retryable ? '#f9c74f' : '#ef6461';
  }
  packets.push({
    serverId: o.clientId,
    color,
    start: performance.now(),
    duration: Math.min(2500, Math.max(400, o.latencyMs * 1.5)),
  });
  if (o.kind === 'success') {
    chartData.get(o.clientId)!.push({ t: Date.now(), latency: o.latencyMs });
  } else {
    log(`${o.clientId} ${o.method}: ${o.error} (${o.latencyMs}ms)`, 'err');
  }
}

// --- latency chart ---------------------------------------------------------

const chart = $<HTMLCanvasElement>('#chart');

function drawChart(): void {
  const ctx = chart.getContext('2d')!;
  const w = (chart.width = chart.clientWidth * devicePixelRatio);
  const h = (chart.height = 160 * devicePixelRatio);
  ctx.clearRect(0, 0, w, h);
  const now = Date.now();
  const windowMs = 60_000;
  let maxLat = 100;
  for (const arr of chartData.values()) {
    while (arr.length > 0 && arr[0]!.t < now - windowMs) arr.shift();
    for (const p of arr) maxLat = Math.max(maxLat, p.latency);
  }
  // Grid: 4 horizontal lines + labels.
  ctx.strokeStyle = '#222c47';
  ctx.fillStyle = '#56648a';
  ctx.font = `${10 * devicePixelRatio}px ui-monospace, monospace`;
  for (let i = 1; i <= 3; i++) {
    const y = h - (h - 14) * (i / 4) - 4;
    ctx.beginPath();
    ctx.moveTo(0, y);
    ctx.lineTo(w, y);
    ctx.stroke();
    ctx.fillText(`${Math.round((maxLat * i) / 4)}ms`, 4, y - 3);
  }
  for (const s of DEMO_SERVERS) {
    const arr = chartData.get(s.id)!;
    if (arr.length === 0) continue;
    ctx.strokeStyle = s.color;
    ctx.lineWidth = 1.5 * devicePixelRatio;
    ctx.beginPath();
    for (let i = 0; i < arr.length; i++) {
      const p = arr[i]!;
      const x = w - ((now - p.t) / windowMs) * w;
      const y = h - (p.latency / maxLat) * (h - 14) - 4;
      if (i === 0) ctx.moveTo(x, y);
      else ctx.lineTo(x, y);
    }
    ctx.stroke();
  }
}

setInterval(drawChart, 500);

// --- traffic generator -----------------------------------------------------

let tipHeight = 1;
let rate = 2;
let trafficTimer: ReturnType<typeof setInterval> | null = null;
let methodCursor = 0;

function oneRequest(): void {
  if (manager.state !== 'running') return;
  const n = methodCursor++;
  const calls: Array<() => Promise<unknown>> = [
    () => manager.call('server.ping'),
    () => manager.call('blockchain.estimatefee', [2]),
    () => manager.call('blockchain.block.header', [Math.floor(Math.random() * (tipHeight + 1))]),
    () => manager.call('blockchain.scripthash.get_balance', [DEMO_SCRIPTHASH]),
  ];
  // Outcomes (incl. failures) arrive via policy.onOutcome; per-call
  // rejections are logged there too, so swallow here.
  void calls[n % calls.length]!().catch(() => undefined);
}

function applyRate(): void {
  if (trafficTimer !== null) clearInterval(trafficTimer);
  trafficTimer = null;
  $('#rate-label').textContent = `${rate}/s`;
  if (rate > 0) trafficTimer = setInterval(oneRequest, 1000 / rate);
}

$<HTMLInputElement>('#rate').addEventListener('input', (ev) => {
  rate = Number((ev.target as HTMLInputElement).value);
  applyRate();
});

// --- global controls -------------------------------------------------------

$<HTMLSelectElement>('#policy').addEventListener('change', (ev) => {
  const name = (ev.target as HTMLSelectElement).value;
  inner = policies[name]!();
  log(`policy → ${name} (connections untouched)`, 'good');
});

const suspendBtn = $<HTMLButtonElement>('#suspend');
suspendBtn.addEventListener('click', () => {
  if (manager.state === 'running') {
    void manager.suspend({ graceMs: 1000 }).then(() => {
      suspendBtn.textContent = 'resume';
      updateManagerBadge();
      log('suspended — sockets closed, subscriptions preserved', 'good');
    });
  } else if (manager.state === 'suspended') {
    void manager.resume().then(() => {
      suspendBtn.textContent = 'suspend';
      updateManagerBadge();
      log('resumed — reconnected, subscriptions replayed', 'good');
    });
  }
});

function updateManagerBadge(): void {
  const badge = $('#manager-state');
  badge.textContent = manager.state;
  badge.className = `badge ${manager.state}`;
}

setInterval(updateManagerBadge, 300);

$<HTMLButtonElement>('#mine').addEventListener('click', () => {
  void (async () => {
    const rpc = async (method: string, params: unknown[]): Promise<unknown> => {
      const res = await fetch('/bitcoind/wallet/ci', {
        method: 'POST',
        headers: { Authorization: `Basic ${btoa('ci:ci')}` },
        body: JSON.stringify({ jsonrpc: '1.0', id: 'demo', method, params }),
      });
      const body = (await res.json()) as { result?: unknown; error?: { message?: string } };
      if (body.error) throw new Error(body.error.message ?? 'rpc error');
      return body.result;
    };
    const addr = (await rpc('getnewaddress', [])) as string;
    await rpc('generatetoaddress', [1, addr]);
    log('mined 1 block — waiting for header notification…');
  })().catch((e: unknown) => log(`mine failed: ${(e as Error).message}`, 'err'));
});

// --- boot ------------------------------------------------------------------

void (async () => {
  log('starting manager…');
  await manager.start();
  updateManagerBadge();
  await manager.headers.subscribe((h) => {
    tipHeight = h.height;
    const tip = $('#tip');
    tip.textContent = `tip: #${h.height}`;
    tip.classList.add('pulse');
    setTimeout(() => tip.classList.remove('pulse'), 400);
    log(`new tip: #${h.height}`, 'good');
  });
  applyRate();
  log('running — subscribed to headers');
})().catch((e: unknown) => log(`boot failed: ${(e as Error).message}`, 'err'));
