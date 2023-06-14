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

| name                   | description                                                                          | required                             | default value |
|------------------------|--------------------------------------------------------------------------------------|--------------------------------------|---------------|
| `url`                  | url to gRPC server.                                                                  | true                                 |               |
| `tls.disable`          | flag to disable mTLS secure connection, set it to `true` for an insecure connection. | false                                | `false`       |
| `tls.server.certPath`  | the server certificate path.                                                         | required if `tls.disable` is `false` |               |
| `tls.server.keyPath`   | the server private key path.                                                         | required if `tls.disable` is `false` |               |
| `tls.CA.certPath`      | the root CA certificate path.                                                        | required if `tls.disable` is `false` |               |

## Planned work
- Add a destination for gRPC server. 