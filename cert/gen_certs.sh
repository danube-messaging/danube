#!/bin/bash
set -e  # Exit immediately if a command exits with a non-zero status

# Cleanup old certificates
rm -f *.pem *.srl *.cnf

# Define file names
CA_KEY="ca-key.pem"
CA_CERT="ca-cert.pem"
SERVER_KEY="server-key.pem"
SERVER_CERT="server-cert.pem"
SERVER_CSR="server-req.pem"
CLIENT_KEY="client-key.pem"
CLIENT_CERT="client-cert.pem"
CLIENT_CSR="client-req.pem"

# Create CA
openssl req -x509 -newkey rsa:4096 -days 365 -nodes -keyout "$CA_KEY" -out "$CA_CERT" \
  -subj "/C=XX/ST=Unknown/L=Unknown/O=Example Org/OU=Example Unit/CN=example.com/emailAddress=admin@example.com"

echo "CA's self-signed certificate:"
openssl x509 -in "$CA_CERT" -noout -text

# Generate Server Key and CSR
openssl req -newkey rsa:4096 -nodes -keyout "$SERVER_KEY" -out "$SERVER_CSR" \
  -subj "/C=XX/ST=Unknown/L=Unknown/O=Example Org/OU=Example Unit/CN=server.example.com/emailAddress=admin@example.com"

# Create Server Config file for extensions
cat > server-ext.cnf <<EOF
subjectAltName = DNS:server.example.com, DNS:*.example.com, IP:127.0.0.1
EOF

# Sign Server Certificate with CA
openssl x509 -req -in "$SERVER_CSR" -days 365 -CA "$CA_CERT" -CAkey "$CA_KEY" -CAcreateserial -out "$SERVER_CERT" \
  -extfile server-ext.cnf

echo "Server's signed certificate:"
openssl x509 -in "$SERVER_CERT" -noout -text

# Generate Client Key and CSR
openssl req -newkey rsa:4096 -nodes -keyout "$CLIENT_KEY" -out "$CLIENT_CSR" \
  -subj "/C=XX/ST=Unknown/L=Unknown/O=Example Org/OU=Example Unit/CN=client.example.com/emailAddress=admin@example.com"

# Create Client Config file for extensions
cat > client-ext.cnf <<EOF
subjectAltName = DNS:client.example.com, DNS:*.example.com, IP:127.0.0.1
EOF

# Sign Client Certificate with CA
openssl x509 -req -in "$CLIENT_CSR" -days 365 -CA "$CA_CERT" -CAkey "$CA_KEY" -CAcreateserial -out "$CLIENT_CERT" \
  -extfile client-ext.cnf

echo "Client's signed certificate:"
openssl x509 -in "$CLIENT_CERT" -noout -text

# Cleanup unnecessary files
rm -f *.srl *.cnf