import sys
import os
import getopt
import logging
import string

from esgcet.model import *
from esgcet.publish import deleteDatasetList, updateThreddsMasterCatalog, reinitializeThredds, DELETE, UNPUBLISH, NO_OPERATION, readDatasetMap, parseDatasetVersionId, generateDatasetVersionId, iterateOverDatasets, UPDATE_OP, publishDatasetList
from esgcet.config import loadConfig, initLogging, registerHandlers
from esgcet.exceptions import *
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, join
from esgcet.messaging import debug, info, warning, error, critical, exception
from esgcet.query import queryDatasetMap


def cleanupCatalogs(arg, dirname, names):
    for name in names:
        base, suffix = os.path.splitext(name)
        if suffix==".xml" and name not in ["catalog.xml"]:
            fullname = os.path.join(dirname, name)
            if not arg.has_key(fullname):
                ans = raw_input("The catalog %s is 'orphaned', delete? [y|n]: "%fullname)
                if ans.lower()=='y':
                    print "Deleting %s"%fullname
                    os.unlink(fullname)

def main(argv):

    try:
        args, lastargs = getopt.getopt(argv, "hi:", ['database-delete', 'database-only', 'echo-sql', 'map=', 'no-republish', 'no-thredds-reinit', 'skip-gateway', 'skip-index', 'las', 'log=', 'rest-api', 'skip-thredds', 'sync-thredds', 'use-list='])
    except getopt.error:
        print sys.exc_value
        return

    deleteAll = False
    datasetMap = None
    deleteDset = False
    unpublishOnGateway = False
    echoSql = False
    init_file = None
    gatewayOp = DELETE
    las = False
    log_filename = None
    republish = True
    restApi = None
    thredds = True
    syncThredds = False
    useList = False
    threddsReinit = True
    for flag, arg in args:
        if flag=='--database-delete':
            deleteDset = True
        elif flag=='--database-only':
            gatewayOp = NO_OPERATION
            thredds = False
            deleteDset = True
        elif flag=='--echo-sql':
            echoSql = True
        elif flag in ['-h', '--help']:
            return
        elif flag=='-i':
            init_file = arg
        elif flag=='--map':
            datasetMap = readDatasetMap(arg)
        elif flag=='--skip-gateway':
            gatewayOp = NO_OPERATION
        elif flag=='--skip-index':
            gatewayOp = NO_OPERATION
        elif flag=='--las':
            las = True
        elif flag=='--log':
            log_filename = arg
        elif flag=='--no-republish':
            republish = False
        elif flag=='--no-thredds-reinit':
            threddsReinit = False
        elif flag=='--rest-api':
            restApi = True
        elif flag=='--skip-thredds':
            thredds = False
        elif flag=='--sync-thredds':
            syncThredds = True
        elif flag=='--use-list':
            useList = True
            useListPath = arg

    if gatewayOp!=NO_OPERATION and unpublishOnGateway:
        gatewayOp = UNPUBLISH

    # Load the configuration and set up a database connection
    config = loadConfig(init_file)
    engine = create_engine(config.getdburl('extract'), echo=echoSql, pool_recycle=3600)
    initLogging('extract', override_sa=engine, log_filename=log_filename)
    Session = sessionmaker(bind=engine, autoflush=True, autocommit=False)

    if config is None:
        raise ESGPublishError("No configuration file found.")
    threddsRoot = config.get('DEFAULT', 'thredds_root')

    # Get the default publication interface (REST or Hessian)
    if restApi is None:
        restApi = config.getboolean('DEFAULT', 'use_rest_api', default=False)

    if datasetMap is None:
        if not useList:
            datasetNames = [parseDatasetVersionId(item) for item in lastargs]
        else:
            if useListPath=='-':
                namelist = sys.stdin
            else:
                namelist = open(useListPath)
            datasetNames = []
            for line in namelist.readlines():
                versionId = parseDatasetVersionId(line.strip())
                datasetNames.append(versionId)
    else:
        datasetNames = datasetMap.keys()
        datasetNames.sort()
    result = deleteDatasetList(datasetNames, Session, gatewayOp, thredds, las, deleteDset, deleteAll=deleteAll, republish=republish, reinitThredds=threddsReinit, restInterface=restApi)

    # Republish previous versions as needed. This will happen if the latest version
    # was deleted from the database, and is not
    # the only version. In this case the previous version will be rescanned to generate the aggregations.
    if republish:
        statusDict, republishList = result
        if len(republishList)>0:

            # Register project handlers.
            registerHandlers()

            info("Republishing modified datasets:")
            republishDatasetNames = [generateDatasetVersionId(dsetTuple) for dsetTuple in republishList]
            dmap, offline = queryDatasetMap(republishDatasetNames, Session)
            datasetNames = dmap.keys()
            datasets = iterateOverDatasets(None, dmap, None, republishList, Session, "time", UPDATE_OP, None, {}, offline, {}, forceAggregate=True)
            republishOp = (gatewayOp != NO_OPERATION) # Don't republish if skipping the gateway op
            result = publishDatasetList(datasetNames, Session, publish=republishOp, thredds=thredds)

    # Synchronize database and THREDDS catalogs
    if syncThredds:
        threddsRoot = config.get('DEFAULT', 'thredds_root')

        # Make a dictionary of catalogs from the database
        session = Session()
        subcatalogs = session.query(Catalog).select_from(join(Catalog, Dataset, Catalog.dataset_name==Dataset.name)).all()
        catdict = {}
        for catalog in subcatalogs:
            location = os.path.join(threddsRoot, catalog.location)
            catdict[location] = 1
        session.close()

        # Scan all XML files in the threddsroot
        os.path.walk(threddsRoot, cleanupCatalogs, catdict)
