// Minimal bitcoind RPC client for the regtest stack. We use it to mine
// blocks (driving subscription-catchup tests) and pull addresses /
// scripthashes for scripthash-subscribe tests.
//
// bitcoind's JSON-RPC contract is `POST /` with `{ method, params }` body
// and HTTP basic auth. Errors come back as `{ error: { code, message } }`
// inside a 200-or-500 response — we surface both as Error throws.

import { INTEGRATION_HOST } from './config.js';

const RPC_PORT = Number(process.env['BITCOIND_RPC_PORT'] ?? '18443');
const USER = process.env['BITCOIND_RPC_USER'] ?? 'ci';
const PASSWORD = process.env['BITCOIND_RPC_PASSWORD'] ?? 'ci';
const URL = `http://${INTEGRATION_HOST}:${RPC_PORT}/`;
const AUTH = `Basic ${Buffer.from(`${USER}:${PASSWORD}`).toString('base64')}`;

let nextId = 1;

export async function rpc<T = unknown>(method: string, params: unknown[] = []): Promise<T> {
  const body = JSON.stringify({ jsonrpc: '1.0', id: nextId++, method, params });
  const res = await fetch(URL, {
    method: 'POST',
    headers: { 'content-type': 'application/json', authorization: AUTH },
    body,
  });
  const text = await res.text();
  let payload: { result?: T; error?: { code: number; message: string } };
  try {
    payload = JSON.parse(text);
  } catch {
    throw new Error(`bitcoind ${method} returned non-JSON (status ${res.status}): ${text}`);
  }
  if (payload.error) {
    throw new Error(`bitcoind ${method} error ${payload.error.code}: ${payload.error.message}`);
  }
  return payload.result as T;
}

/** Generate `count` blocks to a fresh wallet address. Returns the block hashes. */
export async function mineBlocks(count: number): Promise<string[]> {
  // `getnewaddress` requires a loaded wallet. CI's bitcoind starts without
  // one; create on demand. Idempotent: -25 means already exists / loaded.
  await ensureWallet();
  const addr = await rpc<string>('getnewaddress', []);
  return rpc<string[]>('generatetoaddress', [count, addr]);
}

async function ensureWallet(): Promise<void> {
  try {
    await rpc('createwallet', ['ci']);
  } catch (e) {
    // -4 / "Database already exists" / "Wallet ... already exists" — fine.
    if (e instanceof Error && /already (exists|loaded)/.test(e.message)) return;
    // -32601 method not found is a different shape — re-throw.
    if (e instanceof Error && /already loaded|already exists/i.test(e.message)) return;
    if (e instanceof Error && /-4/.test(e.message)) return;
    throw e;
  }
}

/** Current chain tip height per bitcoind. */
export async function getBlockCount(): Promise<number> {
  return rpc<number>('getblockcount', []);
}
