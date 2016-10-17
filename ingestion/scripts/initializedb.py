import os
import sys
import transaction

from sqlalchemy import engine_from_config

from pyramid.paster import (
    get_appsettings,
    setup_logging,
    )

from pyramid.scripts.common import parse_vars

from ..models import (
    DBSession,
    Base,
    FacetName,
    )


def usage(argv):
    cmd = os.path.basename(argv[0])
    print('usage: %s <config_uri> [var=value]\n'
          '(example: "%s development.ini")' % (cmd, cmd))
    sys.exit(1)


def main(argv=sys.argv):
    if len(argv) < 2:
        usage(argv)
    config_uri = argv[1]
    options = parse_vars(argv[2:])
    setup_logging(config_uri)
    settings = get_appsettings(config_uri, options=options)
    engine = engine_from_config(settings, 'sqlalchemy.')
    DBSession.configure(bind=engine)
    Base.metadata.create_all(engine)
    with transaction.manager:
        DBSession.add(FacetName(name="project"))
        DBSession.add(FacetName(name="data_type"))
        DBSession.add(FacetName(name="institute"))
        DBSession.add(FacetName(name="product"))
        DBSession.add(FacetName(name="model"))
        DBSession.add(FacetName(name="instrument"))
        DBSession.add(FacetName(name="experiment_family"))
        DBSession.add(FacetName(name="experiment"))
        DBSession.add(FacetName(name="versionnum"))
        DBSession.add(FacetName(name="realm"))
        DBSession.add(FacetName(name="regridding"))
        DBSession.add(FacetName(name="time_frequency"))
        DBSession.add(FacetName(name="grid_resolution"))
        DBSession.add(FacetName(name="years_spanned"))
        DBSession.add(FacetName(name="variable"))
        DBSession.add(FacetName(name="variable_long_name"))
        DBSession.add(FacetName(name="cf_standard_name"))
        DBSession.add(FacetName(name="ensemble"))
        DBSession.add(FacetName(name="data_node"))
        DBSession.add(FacetName(name="range"))
