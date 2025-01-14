# Conduit Connector for gRPC Server
The gRPC Server connector is one of [Conduit](https://conduit.io) plugins. It provides a source gRPC Server connector.

This connector should be paired with another Conduit instance or pipeline, that provides a
[gRPC client destination](https://github.com/conduitio-labs/conduit-connector-grpc-client). Where the client will initiate
the connection with this server, and start sending records to it.

## How to build?
Run `make build` to build the connector.

## Testing
Run `make test` to run all the unit tests.

## Source
This source connector creates a server on the `url` provided as a parameter. When a client initiates connection, a
bidirectional gRPC stream is created between the server and the client, the server keeps listening on this stream to
receive records sent from the client, when a record is received, an acknowledgment is sent to the client on the same
stream.

### Configuration

| name                   | description                                                                            | required                               | default value |
|------------------------|----------------------------------------------------------------------------------------|----------------------------------------|---------------|
| `url`                  | url to gRPC server.                                                                    | true                                   |               |
| `mtls.disabled`        | option to disable mTLS secure connection, set it to `true` for an insecure connection. | false                                  | `false`       |
| `mtls.server.certPath` | the server certificate path.                                                           | required if `mtls.disabled` is `false` |               |
| `mtls.server.keyPath`  | the server private key path.                                                           | required if `mtls.disabled` is `false` |               |
| `mtls.ca.certPath`     | the root CA certificate path.                                                          | required if `mtls.disabled` is `false` |               |

## Mutual TLS (mTLS)
Mutual TLS is used by default to connect to the server, to disable mTLS you can set the parameter `mtls.disabled`
to `true`, this will result in an insecure connection to the server.

This repo contains self-signed certificates that can be used for local testing purposes, you can find them
under `./test/certs`, note that these certificates are not meant to be used in production environment.

To generate your own secure mTLS certificates, check
[this tutorial](https://medium.com/weekly-webtips/how-to-generate-keys-for-mutual-tls-authentication-a90f53bcec64).

## Planned work
- Add a destination for gRPC server. 

![scarf pixel](https://static.scarf.sh/a.png?x-pxid=fdabe747-b944-4ab3-a349-b8d3e2f2e19c)