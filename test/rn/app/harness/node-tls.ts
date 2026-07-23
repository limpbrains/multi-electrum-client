// node:tls stand-in backed by react-native-tcp-socket, wired up via a
// Metro alias. src/transport/tls.ts imports `connect` and waits for the
// socket's 'secureConnect' event — connectTLS emits exactly that.
// (The unit suite itself never opens a real TLS connection; every
// tls.test.ts case injects a mock socket.)
import TcpSocket from 'react-native-tcp-socket';

type ConnectTlsOptions = Parameters<typeof TcpSocket.connectTLS>[0];

export const connect = (options: ConnectTlsOptions): ReturnType<typeof TcpSocket.connectTLS> =>
  TcpSocket.connectTLS(options);

export const TLSSocket = TcpSocket.TLSSocket;

export default { connect, TLSSocket };
