#!/bin/bash

# create a directory to store the generated keys and certificates
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
mkdir -p ${SCRIPT_DIR}/certs
rm -rf ${SCRIPT_DIR}/certs/*.*
cd ${SCRIPT_DIR}/certs || exit

# generate CA private key
openssl genpkey -algorithm RSA -out ca.key

# generate CA self-signed certificate
openssl req -x509 -new -nodes -key ca.key -sha256 -days 365 -out ca.crt -subj "/CN=ca"

# generate server private key and certificate
openssl req -newkey rsa:2048 -nodes -keyout server.key -subj "/CN=localhost" -out server.csr
echo "subjectAltName = DNS:localhost, IP:127.0.0.1" > extfile.cnf
openssl x509 -req -in server.csr -CA ca.crt -CAkey ca.key -CAcreateserial -out server.crt -days 365 -extfile extfile.cnf

# generate client private key and certificate
openssl req -newkey rsa:2048 -nodes -keyout client.key -subj "/CN=client" -out client.csr
openssl x509 -req -in client.csr -CA ca.crt -CAkey ca.key -CAcreateserial -out client.crt -days 365

# cleanup tmp files
rm server.csr client.csr ca.srl extfile.cnf

echo "Keys and certificates generated successfully."

