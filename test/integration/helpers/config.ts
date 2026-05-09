// Environment-driven config for the integration suite. CI overrides
// `INTEGRATION_HOST` when the compose stack runs in a sibling container
// (Docker-in-Docker / network=host quirks); the default targets a stack
// brought up via `docker compose -f docker/compose.yml --profile slim up`.

export const INTEGRATION_HOST = process.env['INTEGRATION_HOST'] ?? '127.0.0.1';

/** Direct lanes — port-mapped 1:1 from the compose service. */
export const PORTS = {
  electrumxTcp: Number(process.env['ELECTRUMX_TCP_PORT'] ?? '50001'),
  fulcrumTcp: Number(process.env['FULCRUM_TCP_PORT'] ?? '50101'),
  electrsTcp: Number(process.env['ELECTRS_TCP_PORT'] ?? '50201'),
  toxiproxyAdmin: Number(process.env['TOXIPROXY_PORT'] ?? '8474'),
  toxiproxyElectrumxTcp: Number(process.env['TOXI_ELECTRUMX_TCP_PORT'] ?? '60001'),
  toxiproxyFulcrumTcp: Number(process.env['TOXI_FULCRUM_TCP_PORT'] ?? '60101'),
  toxiproxyElectrsTcp: Number(process.env['TOXI_ELECTRS_TCP_PORT'] ?? '60201'),
};
