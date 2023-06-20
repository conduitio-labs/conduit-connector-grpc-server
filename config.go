// Copyright © 2023 Meroxa, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package grpcserver

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"
)

// Config has the generic parameters needed for a gRPC server
type Config struct {
	// url to gRPC server
	URL string `json:"url" validate:"required"`
	// the server certificate path.
	TLSServerCertPath string `json:"tls.server.certPath"`
	// the server private key path.
	TLSServerKeyPath string `json:"tls.server.keyPath"`
	// the root CA certificate path.
	TLSCACertPath string `json:"tls.CA.certPath"`
	// flag to disable mTLS secure connection, set it to `true` for an insecure connection.
	TLSDisable bool `json:"tls.disable" default:"false"`
}

// ParseMTLSFiles parses and validates mTLS params values, returns the parsed server certificate, and CA certificate pool,
// and an error if the parsing fails
func (c *Config) ParseMTLSFiles() (tls.Certificate, *x509.CertPool, error) {
	if c.TLSCACertPath == "" || c.TLSServerCertPath == "" || c.TLSServerKeyPath == "" {
		return tls.Certificate{}, nil, fmt.Errorf("mTLS security is enabled, %q & %q & %q must all be provided, if you wish to disable mTLS"+
			" secure connection, set the %q flag to true", "tls.server.certPath", "tls.server.keyPath", "tls.CA.certPath", "tls.disable")
	}
	serverCert, err := tls.LoadX509KeyPair(c.TLSServerCertPath, c.TLSServerKeyPath)
	if err != nil {
		return tls.Certificate{}, nil, fmt.Errorf("failed to load server key pair: %w", err)
	}
	// Load CA certificate
	caCert, err := os.ReadFile(c.TLSCACertPath)
	if err != nil {
		return tls.Certificate{}, nil, fmt.Errorf("failed to read CA certificate: %w", err)
	}
	caCertPool := x509.NewCertPool()
	if ok := caCertPool.AppendCertsFromPEM(caCert); !ok {
		return tls.Certificate{}, nil, errors.New("failed to append CA certs")
	}
	return serverCert, caCertPool, nil
}
