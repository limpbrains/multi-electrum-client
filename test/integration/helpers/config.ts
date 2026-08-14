// Environment-driven config for the integration suite. CI overrides
// `INTEGRATION_HOST` when the compose stack runs in a sibling container
// (Docker-in-Docker / network=host quirks); the default targets a stack
// brought up via `docker compose -f docker/compose.yml --profile slim up`.

export const INTEGRATION_HOST = process.env['INTEGRATION_HOST'] ?? '127.0.0.1';

/** Direct lanes — port-mapped 1:1 from the compose service. */
export const PORTS = {
  electrumxTcp: Number(process.env['ELECTRUMX_TCP_PORT'] ?? '50001'),
  fulcrumTcp: Number(process.env['FULCRUM_TCP_PORT'] ?? '50101'),
  fulcrumStrictTcp: Number(process.env['FULCRUM_STRICT_TCP_PORT'] ?? '50102'),
  electrsTcp: Number(process.env['ELECTRS_TCP_PORT'] ?? '50201'),
  toxiproxyAdmin: Number(process.env['TOXIPROXY_PORT'] ?? '8474'),
  toxiproxyElectrumxTcp: Number(process.env['TOXI_ELECTRUMX_TCP_PORT'] ?? '52001'),
  toxiproxyFulcrumTcp: Number(process.env['TOXI_FULCRUM_TCP_PORT'] ?? '52101'),
  toxiproxyElectrsTcp: Number(process.env['TOXI_ELECTRS_TCP_PORT'] ?? '52201'),
};

/**
 * Lane selector: the SAME suite runs over TCP (default) and WebSocket —
 * `INTEGRATION_LANE=ws pnpm test:integration`. Every `Protocol` the
 * library ships gets a real-server integration lane (issue #28).
 *
 * WS lane endpoints:
 *  - electrumx / fulcrum / fulcrum-strict: NATIVE Electrum-over-WebSocket
 *    (one JSON-RPC payload per message, no trailing newline — the
 *    default `wsFraming: 'message'`).
 *  - electrs has no native WebSocket: its ws lane is the ws↔tcp byte
 *    bridge (`wsFraming: 'newline'`), which ALSO gives the tunnel
 *    framing real-server coverage. The bridge dials THROUGH toxiproxy
 *    (`electrs-ws`), so even the "direct" electrs ws endpoint is
 *    structurally proxy-routed — deliberate, do not "fix": it lets the
 *    same proxy name sever the lane end-to-end.
 */
export type Lane = 'tcp' | 'ws';
export const LANE: Lane = process.env['INTEGRATION_LANE'] === 'ws' ? 'ws' : 'tcp';

export type LaneServer = 'electrumx' | 'fulcrum' | 'fulcrum-strict' | 'electrs';

export interface LaneEndpoint {
  host: string;
  port: number;
  protocol: 'tcp' | 'ws';
  wsFraming?: 'message' | 'newline';
}

const WS_PORTS = {
  electrumx: Number(process.env['ELECTRUMX_WS_PORT'] ?? '50003'),
  fulcrum: Number(process.env['FULCRUM_WS_PORT'] ?? '50103'),
  fulcrumStrict: Number(process.env['FULCRUM_STRICT_WS_PORT'] ?? '50104'),
  electrsBridge: Number(process.env['ELECTRS_WS_PORT'] ?? '50203'),
  toxiproxyElectrumx: Number(process.env['TOXI_ELECTRUMX_WS_PORT'] ?? '52003'),
  toxiproxyFulcrum: Number(process.env['TOXI_FULCRUM_WS_PORT'] ?? '52103'),
};

interface LaneRow {
  direct: number;
  proxy?: number;
  proxyName?: string;
  wsFraming?: 'message' | 'newline';
}

const LANES: Record<Lane, Record<LaneServer, LaneRow>> = {
  tcp: {
    electrumx: {
      direct: PORTS.electrumxTcp,
      proxy: PORTS.toxiproxyElectrumxTcp,
      proxyName: 'electrumx-tcp',
    },
    fulcrum: {
      direct: PORTS.fulcrumTcp,
      proxy: PORTS.toxiproxyFulcrumTcp,
      proxyName: 'fulcrum-tcp',
    },
    'fulcrum-strict': { direct: PORTS.fulcrumStrictTcp },
    electrs: {
      direct: PORTS.electrsTcp,
      proxy: PORTS.toxiproxyElectrsTcp,
      proxyName: 'electrs-tcp',
    },
  },
  ws: {
    electrumx: {
      direct: WS_PORTS.electrumx,
      proxy: WS_PORTS.toxiproxyElectrumx,
      proxyName: 'electrumx-ws',
      wsFraming: 'message',
    },
    fulcrum: {
      direct: WS_PORTS.fulcrum,
      proxy: WS_PORTS.toxiproxyFulcrum,
      proxyName: 'fulcrum-ws',
      wsFraming: 'message',
    },
    'fulcrum-strict': { direct: WS_PORTS.fulcrumStrict, wsFraming: 'message' },
    // The bridge is the endpoint; toxiproxy sits between it and electrs.
    electrs: {
      direct: WS_PORTS.electrsBridge,
      proxy: WS_PORTS.electrsBridge,
      proxyName: 'electrs-ws',
      wsFraming: 'newline',
    },
  },
};

export const lane = {
  /** Endpoint spec fragment for `server` on the active lane. */
  spec(server: LaneServer, opts?: { via?: 'direct' | 'proxy' }): LaneEndpoint {
    const row = LANES[LANE][server];
    const via = opts?.via ?? 'direct';
    const port = via === 'proxy' ? row.proxy : row.direct;
    if (port === undefined) {
      throw new Error(`no ${via} ${LANE} lane for ${server}`);
    }
    return {
      host: INTEGRATION_HOST,
      port,
      protocol: LANE,
      ...(row.wsFraming !== undefined ? { wsFraming: row.wsFraming } : {}),
    };
  },
  /** Toxiproxy proxy name for `server` on the active lane. */
  proxy(server: LaneServer): string {
    const name = LANES[LANE][server].proxyName;
    if (name === undefined) throw new Error(`no ${LANE} proxy for ${server}`);
    return name;
  },
};
