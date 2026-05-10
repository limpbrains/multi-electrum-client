// One-shot side-channel poll against ElectrumX. Used by tests that
// need to observe ElectrumX's view of the chain WITHOUT going through
// the manager (e.g. while the manager is suspended or its lane is
// proxied off via toxiproxy). Always hits the direct port (not the
// toxiproxy lane).

import { createConnection } from 'node:net';

import { INTEGRATION_HOST, PORTS } from './config.js';

/**
 * Open a TCP socket to ElectrumX, send `headers.subscribe`, read one
 * reply, close. Returns true iff the reported tip is at least
 * `targetHeight`. Returns false on any error / timeout — caller
 * polls via `waitFor` to retry.
 */
export async function electrumxKnowsHeight(targetHeight: number): Promise<boolean> {
  return new Promise<boolean>((resolve) => {
    const sock = createConnection(PORTS.electrumxTcp, INTEGRATION_HOST);
    let buf = '';
    const cleanup = (result: boolean): void => {
      sock.destroy();
      resolve(result);
    };
    sock.setTimeout(3000);
    sock.on('connect', () => {
      sock.write(
        JSON.stringify({ id: 1, method: 'blockchain.headers.subscribe', params: [] }) + '\n',
      );
    });
    sock.on('data', (chunk) => {
      buf += chunk.toString('utf8');
      const nl = buf.indexOf('\n');
      if (nl < 0) return;
      try {
        const msg = JSON.parse(buf.slice(0, nl));
        const height = msg?.result?.height;
        cleanup(typeof height === 'number' && height >= targetHeight);
      } catch {
        cleanup(false);
      }
    });
    sock.on('error', () => cleanup(false));
    sock.on('timeout', () => cleanup(false));
  });
}
