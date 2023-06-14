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
	"testing"
)

func TestConfig_ParseMTLSFiles(t *testing.T) {
	testCases := []struct {
		name    string
		config  Config
		wantErr bool
	}{
		{
			name: "valid paths",
			config: Config{
				TLSServerCertPath: "./test/certs/server.crt",
				TLSServerKeyPath:  "./test/certs/server.key",
				TLSCACertPath:     "./test/certs/ca.crt",
			},
			wantErr: false,
		},
		{
			name: "empty values",
			config: Config{
				TLSServerCertPath: "",
				TLSServerKeyPath:  "",
				TLSCACertPath:     "",
			},
			wantErr: true,
		},
		{
			name: "invalid paths",
			config: Config{
				TLSServerCertPath: "not a file",
				TLSServerKeyPath:  "not a file",
				TLSCACertPath:     "not a file",
			},
			wantErr: true,
		},
		{
			name: "switched files",
			config: Config{
				TLSServerCertPath: "./test/certs/server.key", // switched with server crt
				TLSServerKeyPath:  "./test/certs/server.crt",
				TLSCACertPath:     "./test/certs/ca.crt",
			},
			wantErr: true,
		},
		{
			name: "wrong CA cert path",
			config: Config{
				TLSServerCertPath: "./test/certs/server.crt",
				TLSServerKeyPath:  "./test/certs/server.key",
				TLSCACertPath:     "./test/certs/ca.key", // key instead of crt, should fail
			},
			wantErr: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			_, _, err := tc.config.ParseMTLSFiles()
			if (err != nil) != tc.wantErr {
				t.Errorf("ParseMTLSFiles() error = %v, wantErr = %v", err, tc.wantErr)
			}
		})
	}
}
