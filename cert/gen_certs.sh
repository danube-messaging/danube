#!/bin/bash
set -e

# Generate CA
openssl req -x509 -newkey rsa:4096 -nodes -keyout ca-key.pem -out ca-cert.pem \
  -subj "/CN=Danube CA"

# Generate Server Certificate
openssl req -newkey rsa:4096 -nodes -keyout server-key.pem -out server-req.pem \
  -subj "/CN=localhost"

# Sign with proper SANs
echo "subjectAltName=DNS:localhost,IP:127.0.0.1,IP:0.0.0.0" > server-ext.cnf
openssl x509 -req -in server-req.pem -days 365 -CA ca-cert.pem -CAkey ca-key.pem \
  -CAcreateserial -out server-cert.pem -extfile server-ext.cnf

# Generate Client Certificate
openssl req -newkey rsa:4096 -nodes -keyout client-key.pem -out client-req.pem \
  -subj "/CN=danube-client"

openssl x509 -req -in client-req.pem -days 365 -CA ca-cert.pem -CAkey ca-key.pem \
  -CAcreateserial -out client-cert.pem

rm -f *.srl *.cnf server-req.pem client-req.pem