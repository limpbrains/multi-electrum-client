// React Native example. Pair `manager.suspend()` / `resume()` with RN's
// `AppState` via the `bindAppState` helper so the manager hibernates when
// the app backgrounds. Requires:
//
//   pnpm add multi-electrum-client react-native-tcp-socket
//
// And a metro alias mapping `node:net` and `node:tls` to
// `react-native-tcp-socket`. See `react-native-tcp-socket`'s README for the
// canonical metro config.

import { useEffect, useState } from 'react';
import { AppState, Text, View } from 'react-native';

import { ElectrumManager, bindAppState, failover, type BlockHeader } from 'multi-electrum-client';

export function ChainTip(): JSX.Element {
  const [tip, setTip] = useState<BlockHeader | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    const manager = new ElectrumManager({
      network: 'mainnet',
      servers: [
        { id: 'a', host: 'electrum.example.org', port: 50002, protocol: 'tls' },
        { id: 'b', host: 'electrum2.example.org', port: 50002, protocol: 'tls' },
      ],
      policy: failover(['a', 'b']),
    });
    manager.on('error', (e) => setError(String(e)));

    let unsubHeaders: (() => Promise<void>) | null = null;
    let disposeAppState: (() => void) | null = null;
    void (async () => {
      await manager.start();
      unsubHeaders = await manager.headers.subscribe(setTip);
      disposeAppState = bindAppState(manager, AppState);
    })();

    return () => {
      void (async () => {
        try {
          if (unsubHeaders) await unsubHeaders();
          disposeAppState?.();
          await manager.stop();
        } catch {
          // ignore teardown errors
        }
      })();
    };
  }, []);

  return (
    <View>
      <Text>{error ? `error: ${error}` : tip ? `tip: ${tip.height}` : 'connecting…'}</Text>
    </View>
  );
}
