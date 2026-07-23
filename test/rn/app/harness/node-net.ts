// node:net stand-in backed by react-native-tcp-socket, wired up via a
// Metro alias. react-native-tcp-socket intentionally mirrors node's API:
// `connect(options)`, `createServer(listener)`, positional
// `server.listen(port, host, cb)`, and `server.address()` all behave like
// node:net, so this is a re-export rather than an adapter.
import TcpSocket from 'react-native-tcp-socket';

export const connect = TcpSocket.connect;
export const createConnection = TcpSocket.createConnection;
export const createServer = TcpSocket.createServer;
export const isIP = TcpSocket.isIP;
export const isIPv4 = TcpSocket.isIPv4;
export const isIPv6 = TcpSocket.isIPv6;
export const Server = TcpSocket.Server;
export const Socket = TcpSocket.Socket;

export default TcpSocket;
