// Vite config for the interactive browser demo. Run from the repo root:
//
//   docker compose -f docker/compose.yml --profile slim --profile demo up -d --wait
//   pnpm demo
//
// The alias serves the library straight from source (browser entry), so
// no build step is needed and library edits hot-reload. The dev-server
// proxies exist to reach the toxiproxy admin API and bitcoind RPC from
// the page without CORS pain.

import { fileURLToPath } from 'node:url';
import { defineConfig } from 'vite';

export default defineConfig({
  root: fileURLToPath(new URL('.', import.meta.url)),
  resolve: {
    alias: {
      'multi-electrum-client': fileURLToPath(
        new URL('../../src/index.browser.ts', import.meta.url),
      ),
    },
  },
  server: {
    port: 5173,
    proxy: {
      // Toxiproxy admin API (latency toxics, lane cut). Toxiproxy ≥2.6
      // rejects browser requests ("User agent not allowed") as CSRF
      // protection — override the UA so the page can drive it.
      '/toxiproxy': {
        target: 'http://127.0.0.1:8474',
        rewrite: (p) => p.replace(/^\/toxiproxy/, ''),
        headers: { 'User-Agent': 'melc-demo-proxy' },
      },
      // bitcoind regtest RPC ("mine block" button). The page sends the
      // Authorization header itself (ci:ci); the proxy just forwards.
      '/bitcoind': {
        target: 'http://127.0.0.1:18443',
        rewrite: (p) => p.replace(/^\/bitcoind/, ''),
      },
    },
  },
});
