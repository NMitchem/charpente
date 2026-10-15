# Test-only TLS material

**Never use any of this in production.** These files exist so the edge harness
(`crates/edge-harness/tests/outbound.rs`) can run `fake_remote` as an HTTPS
remote for `pay.fixture` on loopback, and so `outbound_node` can verify it.

- `ca.pem` / `ca.key`: a self-signed CA, "charpente outbound-fixture test CA",
  with a critical name constraint that permits `pay.fixture` and nothing else
  (`nameConstraints=critical,permitted;DNS:pay.fixture`).
- `server.pem` / `server.key`: a certificate for `pay.fixture` (its only
  subject alternative name), issued by that CA. `fake_remote` serves it.

The constraint is there because `ca.key` is public. `--outbound-ca` trusts a
CA for every host, so a node started with this CA (a harness command line
copied into a real deployment, say) would otherwise accept a certificate any
reader of this repository minted for a host it really calls, and send that
host's credential to it. With the constraint, webpki refuses any certificate
this CA issues for a name other than `pay.fixture`.

Both private keys are committed on purpose: they are public test fixtures,
not secrets, and they protect nothing. The CA is trusted nowhere except where
a test passes it to a node with `--outbound-ca crates/outbound-fixture/tls/ca.pem`.
No operating-system or browser trust store holds it, and nothing in this
repository adds it to one.

## Regenerating

The certificates are valid for 100 years, so this should not be needed. From
the repository root:

```bash
mkdir -p crates/outbound-fixture/tls && cd crates/outbound-fixture/tls
openssl req -x509 -newkey rsa:2048 -nodes -keyout ca.key -out ca.pem -days 36500 \
  -subj "/CN=charpente outbound-fixture test CA" \
  -addext "basicConstraints=critical,CA:TRUE" -addext "keyUsage=critical,keyCertSign,cRLSign" \
  -addext "nameConstraints=critical,permitted;DNS:pay.fixture"
openssl req -newkey rsa:2048 -nodes -keyout server.key -out server.csr -subj "/CN=pay.fixture"
printf "subjectAltName=DNS:pay.fixture\nbasicConstraints=CA:FALSE\nkeyUsage=critical,digitalSignature,keyEncipherment\nextendedKeyUsage=serverAuth\n" > ext.cnf
openssl x509 -req -in server.csr -CA ca.pem -CAkey ca.key -CAcreateserial -out server.pem -days 36500 -extfile ext.cnf
rm server.csr ext.cnf ca.srl
openssl verify -CAfile ca.pem server.pem      # expect: server.pem: OK
openssl x509 -in server.pem -noout -text | grep -A1 "Subject Alternative Name"   # expect: DNS:pay.fixture
openssl x509 -in ca.pem -noout -text | grep -A2 "Name Constraints"   # expect: critical, Permitted: DNS:pay.fixture
```

The permitted name must be exactly the names `server.pem` carries: a name the
server certificate adds needs a matching `permitted;DNS:` entry here, or the
harness's trusted-CA scenarios fail verification.

Regenerating replaces all four files; commit them together.
