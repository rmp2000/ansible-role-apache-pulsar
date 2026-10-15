#!/usr/bin/env python3
"""Functional checks. Read endpoint settings from stdin and credentials from protected files."""
import base64
import json
import logging
from pathlib import Path
import ssl
import sys
import urllib.error
import urllib.request
import uuid

import pulsar

logging.basicConfig(level=logging.CRITICAL)
ROOT = Path('/etc/apache-pulsar')
STATE = Path('/srv/apache-pulsar/.pulsar-verification.json')
CA = str(ROOT/'tls/server.crt')


def main():
    config = json.load(sys.stdin)
    host = config['host']
    admin_url = 'https://'+host+':'+str(config['admin_port'])
    broker_url = 'pulsar+ssl://'+host+':'+str(config['broker_port'])
    admin = (ROOT/'admin.token').read_text().strip()
    token = (ROOT/'client.token').read_text().strip()
    context = ssl.create_default_context(cafile=CA)

    def request(path, method='GET', credential=admin, expected=(200, 204)):
        headers = {'Authorization': 'Bearer '+credential} if credential else {}
        req = urllib.request.Request(admin_url+path, headers=headers, method=method)
        try:
            with urllib.request.urlopen(req, context=context, timeout=15) as response:
                status = response.status
        except urllib.error.HTTPError as error:
            status = error.code
        if status not in expected:
            raise RuntimeError('Unexpected HTTP status '+str(status)+' for '+path)

    def client():
        return pulsar.Client(broker_url, authentication=pulsar.AuthenticationToken(token),
                             tls_trust_certs_file_path=CA,
                             tls_allow_insecure_connection=False,
                             tls_validate_hostname=True,
                             operation_timeout_seconds=15,
                             connection_timeout_ms=5000,
                             logger=logging.getLogger('pulsar-check'))

    def delete(topic):
        path = '/admin/v2/persistent/'+topic.split('://', 1)[1]
        request(path+'?force=true', method='DELETE', expected=(200, 204, 404))

    mode = sys.argv[1]
    if mode == 'smoke':
        request('/admin/v2/tenants', credential=None, expected=(401,))
        request('/admin/v2/tenants', credential='invalid-token', expected=(401,))
        denied = (ROOT/'denied.token').read_text().strip()
        request('/admin/v2/namespaces/'+config['namespace']+'/permissions',
                credential=denied, expected=(403,))
        topic = 'persistent://'+config['namespace']+'/smoke-'+uuid.uuid4().hex
        connection = client()
        try:
            consumer = connection.subscribe(topic, 'verification',
                                            initial_position=pulsar.InitialPosition.Earliest)
            producer = connection.create_producer(topic)
            payloads = [bytes(range(256))*16, uuid.uuid4().bytes]
            for payload in payloads:
                producer.send(payload)
            for expected in payloads:
                msg = consumer.receive(timeout_millis=15000)
                if msg.data() != expected:
                    raise RuntimeError('Message content mismatch')
                consumer.acknowledge(msg)
            consumer.unsubscribe()
        finally:
            connection.close()
            delete(topic)
        print('APACHE_PULSAR_SMOKE_OK: TLS, token authentication, authorization, exact binary messaging and cleanup')
    elif mode == 'stage':
        if STATE.exists():
            raise RuntimeError('A previous persistence check is pending; run check before staging another')
        topic = 'persistent://'+config['namespace']+'/persistence-'+uuid.uuid4().hex
        payload = uuid.uuid4().bytes+bytes(range(256))
        connection = client()
        try:
            consumer = connection.subscribe(topic, 'persistence',
                                            initial_position=pulsar.InitialPosition.Earliest)
            producer = connection.create_producer(topic)
            producer.send(payload)
            STATE.write_text(json.dumps({'topic': topic, 'payload': base64.b64encode(payload).decode()}))
            STATE.chmod(0o600)
            consumer.close()
        finally:
            connection.close()
        print('APACHE_PULSAR_DURABLE_MESSAGE_STAGED')
    elif mode == 'check':
        staged = json.loads(STATE.read_text())
        connection = client()
        try:
            consumer = connection.subscribe(staged['topic'], 'persistence')
            msg = consumer.receive(timeout_millis=15000)
            if msg.data() != base64.b64decode(staged['payload']):
                raise RuntimeError('Persisted message content mismatch')
            consumer.acknowledge(msg)
            consumer.unsubscribe()
        finally:
            connection.close()
        delete(staged['topic'])
        STATE.unlink()
        print('APACHE_PULSAR_PERSISTENCE_OK')
    else:
        raise RuntimeError('Unsupported check mode')


if __name__ == '__main__':
    try:
        main()
    except Exception as error:
        # Do not emit client exception details which could contain credentials.
        detail = str(error) if type(error) is RuntimeError else type(error).__name__
        print('APACHE_PULSAR_CHECK_FAILED: '+detail, file=sys.stderr)
        raise SystemExit(1)
