import os

from setuptools import setup, find_packages

here = os.path.abspath(os.path.dirname(__file__))
with open(os.path.join(here, 'README.txt')) as f:
    README = f.read()
with open(os.path.join(here, 'CHANGES.txt')) as f:
    CHANGES = f.read()

requires = [
    'pyramid',
    'pyramid_chameleon',
    'pyramid_debugtoolbar',
    'pyramid_tm',
    'psycopg2',
    'SQLAlchemy',
    'transaction',
    'zope.sqlalchemy',
    'waitress',
    'pyramid_celery',
    'celery',
    'pyOpenSSL',
    'MyProxyClient',
    'globusonline-transfer-api-client',
    ]

setup(name='ingestion',
      version='0.2',
      description='ingestion',
      long_description=README + '\n\n' + CHANGES,
      classifiers=[
        "Programming Language :: Python",
        "Framework :: Pyramid",
        "Topic :: Internet :: WWW/HTTP",
        "Topic :: Internet :: WWW/HTTP :: WSGI :: Application",
        ],
      author='Lukasz Lacinski @ University of Chicago',
      author_email='lukasz@uchicago.edu',
      url='',
      keywords='web wsgi bfg pylons pyramid',
      packages=find_packages(),
      include_package_data=True,
      zip_safe=False,
      test_suite='ingestion',
      install_requires=requires,
      entry_points="""\
      [paste.app_factory]
      main = ingestion:main
      [console_scripts]
      initialize_ingestion_db = ingestion.scripts.initializedb:main
      """,
      )
