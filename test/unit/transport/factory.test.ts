import { describe, expect, it } from 'vitest';

import { ProtocolError } from '../../../src/errors/types.js';
import {
  defaultTransportFactory,
  registerTransport,
  registeredProtocols,
} from '../../../src/transport/factory.js';

describe('transport factory', () => {
  it('registers ws/wss/tcp/tls in the Node entry', async () => {
    // Importing the package root pulls in all four registrations.
    await import('../../../src/index.js');
    const protocols = registeredProtocols();
    expect(protocols).toContain('ws');
    expect(protocols).toContain('wss');
    expect(protocols).toContain('tcp');
    expect(protocols).toContain('tls');
  });

  it('throws ProtocolError for unregistered protocols', () => {
    expect(() =>
      defaultTransportFactory({ host: 'h', port: 1, protocol: 'imaginary' as 'tcp' }),
    ).toThrow(ProtocolError);
  });

  it('overwrites prior registration', () => {
    const fakes: { calls: number } = { calls: 0 };
    registerTransport('ws', () => {
      fakes.calls++;
      return {
        endpoint: { host: 'h', port: 1, protocol: 'ws' },
        connect: async () => undefined,
        send: async () => undefined,
        close: async () => undefined,
        on: () => () => undefined,
      };
    });
    defaultTransportFactory({ host: 'h', port: 1, protocol: 'ws' });
    expect(fakes.calls).toBe(1);
    // Restore real ws registration so other tests aren't affected.
    void import('../../../src/transport/ws.js');
  });
});
