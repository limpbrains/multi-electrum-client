import { setTimeout as delay } from 'node:timers/promises';

import { describe, expect, it } from 'vitest';

import { ElectrumManager } from '../../../src/manager.js';
import { failover } from '../../../src/policy/builtins.js';
import type { ServerSpec } from '../../../src/protocol/types.js';
import { buildHarness } from '../../helpers/managerHarness.js';

const SERVERS: ServerSpec[] = [{ id: 'a', host: 'a', port: 50001, protocol: 'ws' }];

describe('namespace API → call() → wire', () => {
  it('scripthash.getBalance issues blockchain.scripthash.get_balance', async () => {
    const h = buildHarness();
    const manager = new ElectrumManager({
      network: 'regtest',
      servers: SERVERS,
      policy: failover(),
      transportFactory: h.factory,
      autoBatch: false,
    });
    await manager.start();

    const p = manager.scripthash.getBalance('HASH');
    await delay(0);
    const sent = JSON.parse(h.transports.get('a')!.sent[0]!);
    expect(sent.method).toBe('blockchain.scripthash.get_balance');
    expect(sent.params).toEqual(['HASH']);

    h.reply('a', (req: { id: number }) => ({
      id: req.id,
      result: { confirmed: 100, unconfirmed: 0 },
    }));
    expect(await p).toEqual({ confirmed: 100, unconfirmed: 0 });

    await manager.stop();
  });

  it('transaction.broadcast issues blockchain.transaction.broadcast', async () => {
    const h = buildHarness();
    const manager = new ElectrumManager({
      network: 'regtest',
      servers: SERVERS,
      policy: failover(),
      transportFactory: h.factory,
      autoBatch: false,
    });
    await manager.start();

    const p = manager.transaction.broadcast('deadbeef');
    await delay(0);
    const sent = JSON.parse(h.transports.get('a')!.sent[0]!);
    expect(sent.method).toBe('blockchain.transaction.broadcast');
    expect(sent.params).toEqual(['deadbeef']);

    h.reply('a', (req: { id: number }) => ({ id: req.id, result: 'TXID123' }));
    expect(await p).toBe('TXID123');

    await manager.stop();
  });

  it('transaction.getMerkle passes both txid and height', async () => {
    const h = buildHarness();
    const manager = new ElectrumManager({
      network: 'regtest',
      servers: SERVERS,
      policy: failover(),
      transportFactory: h.factory,
      autoBatch: false,
    });
    await manager.start();

    const p = manager.transaction.getMerkle('TXID', 700_000);
    await delay(0);
    const sent = JSON.parse(h.transports.get('a')!.sent[0]!);
    expect(sent.method).toBe('blockchain.transaction.get_merkle');
    expect(sent.params).toEqual(['TXID', 700_000]);

    h.reply('a', (req: { id: number }) => ({
      id: req.id,
      result: { blockHeight: 700_000, pos: 3, merkle: ['abc', 'def'] },
    }));
    expect(await p).toEqual({ blockHeight: 700_000, pos: 3, merkle: ['abc', 'def'] });

    await manager.stop();
  });

  it('server.ping and headers.getTip issue empty-param requests', async () => {
    const h = buildHarness();
    const manager = new ElectrumManager({
      network: 'regtest',
      servers: SERVERS,
      policy: failover(),
      transportFactory: h.factory,
      autoBatch: false,
    });
    await manager.start();

    const ping = manager.server.ping();
    await delay(0);
    let sent = JSON.parse(h.transports.get('a')!.sent[0]!);
    expect(sent.method).toBe('server.ping');
    expect(sent.params).toEqual([]);
    h.reply('a', (req: { id: number }) => ({ id: req.id, result: null }));
    expect(await ping).toBeNull();

    const sub = manager.headers.getTip();
    await delay(0);
    sent = JSON.parse(h.transports.get('a')!.sent[0]!);
    expect(sent.method).toBe('blockchain.headers.subscribe');
    h.reply('a', (req: { id: number }) => ({
      id: req.id,
      result: { height: 1, hex: '00' },
    }));
    expect(await sub).toEqual({ height: 1, hex: '00' });

    await manager.stop();
  });

  it('transaction.getVerbose passes verbose=true and returns TxVerbose shape', async () => {
    const h = buildHarness();
    const manager = new ElectrumManager({
      network: 'regtest',
      servers: SERVERS,
      policy: failover(),
      transportFactory: h.factory,
      autoBatch: false,
    });
    await manager.start();

    const p = manager.transaction.getVerbose('TXID');
    await delay(0);
    const sent = JSON.parse(h.transports.get('a')!.sent[0]!);
    expect(sent.method).toBe('blockchain.transaction.get');
    expect(sent.params).toEqual(['TXID', true]);

    h.reply('a', (req: { id: number }) => ({
      id: req.id,
      result: {
        txid: 'TXID',
        hash: 'TXID',
        hex: 'deadbeef',
        size: 100,
        vsize: 80,
        weight: 320,
        version: 2,
        locktime: 0,
        vin: [],
        vout: [],
      },
    }));
    const result = await p;
    expect(result.txid).toBe('TXID');
    expect(result.hex).toBe('deadbeef');

    await manager.stop();
  });

  it('estimateFee passes the confirmation target', async () => {
    const h = buildHarness();
    const manager = new ElectrumManager({
      network: 'regtest',
      servers: SERVERS,
      policy: failover(),
      transportFactory: h.factory,
      autoBatch: false,
    });
    await manager.start();

    const p = manager.estimateFee(6);
    await delay(0);
    const sent = JSON.parse(h.transports.get('a')!.sent[0]!);
    expect(sent.method).toBe('blockchain.estimatefee');
    expect(sent.params).toEqual([6]);

    h.reply('a', (req: { id: number }) => ({ id: req.id, result: 0.0001 }));
    expect(await p).toBe(0.0001);

    await manager.stop();
  });
});
