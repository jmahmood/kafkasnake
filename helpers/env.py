import json
import logging
import os
import subprocess
from urllib.parse import urlparse


def load_heroku_environmental_variables(
        env_vars=('KAFKA_PREFIX', 'KAFKA_CLIENT_CERT', 'KAFKA_CLIENT_CERT_KEY', 'DATABASE_URL', 'KAFKA_URL',
                  'KAFKA_TRUSTED_CERT')):
    """Loads all environmental variables of importance into os.environ from your Heroku environment"""
    if os.environ.get('HEROKU_STAGING_APP_NAME') is None:
        v = input("Please enter the name of your Heroku app with Kafka attached to it: ")
        os.environ['HEROKU_STAGING_APP_NAME'] = v.rstrip()

    try:
        cmd = 'heroku config --json -a {0}'.format(os.environ.get('HEROKU_STAGING_APP_NAME')).split(' ')
        sbp = subprocess.run(cmd, stdout=subprocess.PIPE)
        raw = sbp.stdout.decode('utf8')
        js_raw = json.loads(raw.replace('\n', ''))

        for v in env_vars:
            os.environ.setdefault(v, js_raw.get(v))
    except json.decoder.JSONDecodeError as e:
        # You probably put in the wrong name
        raise LookupError('Error while loading data.  Are you sure {0} is the correct Heroku app name?'.format(
            os.environ.get('HEROKU_STAGING_APP_NAME')))
