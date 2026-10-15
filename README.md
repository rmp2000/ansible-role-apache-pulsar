# Apache Pulsar Ansible role

Installs Apache Pulsar 5.0.0 standalone on Ubuntu 22.04 using the official binary
archive and its pinned SHA-512 checksum, Java 21 and systemd. A standalone JVM
contains the broker, embedded BookKeeper and RocksDB metadata; this is a minimum
test deployment, not a highly available cluster.

Set `apache_pulsar_token_secret` to a secret of at least 32 characters and
`apache_pulsar_bind_address` to the private VM address. TLS is mandatory on 6651
and HTTPS administration on 8443; plaintext listeners are disabled. The generated
self-signed certificate must be trusted by clients. Broker ports should stay on
a private network accessible only to trusted clients.

The `admin` role manages the cluster. The `client` role can produce/consume in
`platform/events`. A `denied` role exists only to test authorization rejection.
Tokens and signing key are stored under `/etc/apache-pulsar` with restricted
permissions; obtain only `client.token` for application clients. Keep the signing
secret unchanged during reconfiguration. Tokens do not expire automatically;
credential lifecycle and certificate renewal require explicit administration.

`apache_pulsar_data_dir` defaults to `/srv/apache-pulsar`, containing both metadata
and BookKeeper data. The TOSCA recipe requires a separate filesystem of at least
1 GiB (`apache_pulsar_require_data_mount: true`). Preserve all of this disk for
recovery. An existing installation is never wiped or upgraded automatically.

The role runs a real binary producer/consumer test over verified TLS, checks
missing and invalid credentials, checks an authenticated unauthorized role and
removes its own topic. Set `apache_pulsar_test_restart: true` to additionally stage
an unconsumed durable message, restart the service, consume the exact original
bytes and remove the temporary topic. A failed persistence test leaves a marker
at `/srv/apache-pulsar/.pulsar-verification.json` for diagnosis/retry.

The Python client is pinned to 3.13.0 in its own virtual environment. Defaults,
templates and handlers are independent of Infrastructure Manager; the matching
TOSCA recipe supplies the cloud topology and inputs.
