import os
from pyramid.paster import get_app, setup_logging

os.environ['X509_CERT_DIR'] = '/etc/grid-security/certificates'

ini_path = '/usr/local/ingestion/ingestion/production.ini'
setup_logging(ini_path)
application = get_app(ini_path, 'main')
