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
    // Use a synthetic protocol so we don't have to restore a real
    // transport's registration (the underlying module is already cached;
    // re-importing it does not re-fire the side-effect).
    const fakes: { calls: number } = { calls: 0 };
    const FAKE = 'fake-test-only' as const;
    const ctor = (): ReturnType<Parameters<typeof registerTransport>[1]> => {
      fakes.calls++;
      return {
        endpoint: { host: 'h', port: 1, protocol: FAKE as unknown as 'ws' },
        connect: async () => undefined,
        send: async () => undefined,
        close: async () => undefined,
        on: () => () => undefined,
      };
    };
    registerTransport(FAKE, ctor);
    defaultTransportFactory({
      host: 'h',
      port: 1,
      protocol: FAKE as unknown as 'ws',
    });
    expect(fakes.calls).toBe(1);

    // Replace and verify the second ctor wins.
    let secondCalls = 0;
    registerTransport(FAKE, () => {
      secondCalls++;
      return {
        endpoint: { host: 'h', port: 1, protocol: FAKE as unknown as 'ws' },
        connect: async () => undefined,
        send: async () => undefined,
        close: async () => undefined,
        on: () => () => undefined,
      };
    });
    defaultTransportFactory({
      host: 'h',
      port: 1,
      protocol: FAKE as unknown as 'ws',
    });
    expect(secondCalls).toBe(1);
    expect(fakes.calls).toBe(1); // first ctor not called again
  });
});
