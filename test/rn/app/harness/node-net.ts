// node:net stand-in backed by react-native-tcp-socket, wired up via a
// Metro alias. react-native-tcp-socket mirrors most of node's API 1:1
// (`createServer(listener)`, positional `server.listen(port, host, cb)`,
// `server.address()`), with one gap: its `connect`/`createConnection` only
// accept an options object, while node also allows the positional
// `(port[, host][, connectListener])` form (used by
// test/integration/helpers/electrumxPoll.ts). Normalize here.
import TcpSocket from 'react-native-tcp-socket';

type ConnectOptions = Parameters<typeof TcpSocket.createConnection>[0];
type ConnectCallback = () => void;

const normalizeConnectArgs = (
  optionsOrPort: ConnectOptions | number,
  hostOrCallback?: string | ConnectCallback,
  maybeCallback?: ConnectCallback,
): [ConnectOptions, ConnectCallback | undefined] => {
  if (typeof optionsOrPort === 'number') {
    const host = typeof hostOrCallback === 'string' ? hostOrCallback : undefined;
    const callback = typeof hostOrCallback === 'function' ? hostOrCallback : maybeCallback;
    return [{ port: optionsOrPort, ...(host !== undefined ? { host } : {}) } as ConnectOptions, callback];
  }
  return [optionsOrPort, typeof hostOrCallback === 'function' ? hostOrCallback : maybeCallback];
};

export function createConnection(
  optionsOrPort: ConnectOptions | number,
  hostOrCallback?: string | ConnectCallback,
  maybeCallback?: ConnectCallback,
): ReturnType<typeof TcpSocket.createConnection> {
  const [options, callback] = normalizeConnectArgs(optionsOrPort, hostOrCallback, maybeCallback);
  return TcpSocket.createConnection(options, callback as ConnectCallback);
}

export const connect = createConnection;
export const createServer = TcpSocket.createServer;
export const isIP = TcpSocket.isIP;
export const isIPv4 = TcpSocket.isIPv4;
export const isIPv6 = TcpSocket.isIPv6;
export const Server = TcpSocket.Server;
export const Socket = TcpSocket.Socket;

export default {
  ...TcpSocket,
  connect,
  createConnection,
};
