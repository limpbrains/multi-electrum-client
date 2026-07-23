// multi-electrum-client — Node / RN / Bun entry. Imports every transport
// for side-effect registration in the factory; the browser entry
// (`src/index.browser.ts`) skips `tcp` / `tls` to keep `node:net` /
// `node:tls` out of bundler resolution graphs. Everything else lives in
// `src/index.core.ts`, shared by both entries.

import './transport/tcp.js';
import './transport/tls.js';

export * from './index.core.js';

// Node-only transport surface (absent from the browser entry):
export { TcpTransport, type TcpTransportOpts, type TcpSocketLike } from './transport/tcp.js';
export { TlsTransport, type TlsTransportOpts } from './transport/tls.js';
