import sys
import logging
import os
import getopt
import string
import stat
import re

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from esgcet.publish import extractFromDataset, aggregateVariables, filelistIterator, fnmatchIterator, fnIterator, directoryIterator, multiDirectoryIterator, progressCallback, StopEvent, readDatasetMap, datasetMapIterator, iterateOverDatasets, publishDatasetList, processIterator, processNodeMatchIterator, CREATE_OP, DELETE_OP, RENAME_OP, UPDATE_OP, REPLACE_OP
from esgcet.config import loadConfig, getHandler, getHandlerByName, initLogging, registerHandlers, splitLine, getOfflineLister
from esgcet.exceptions import *
from esgcet.messaging import debug, info, warning, error, critical, exception
from esgcet.model import Dataset
from esgcet.query import queryDatasetMap


def main(argv):

    try:
        args, lastargs = getopt.getopt(argv, "a:cdehi:m:p:ru", ['append', 'create', 'dataset=', 'delete-files', 'echo-sql', 'experiment=', 'filter=', 'help', 'keep-version', 'log=', 'map=', 'message=', 'model=', 'offline',  'parent=', 'per-time', 'per-variable', 'project=', 'property=', 'publish', 'new-version=', 'no-thredds-reinit', 'noscan', 'read-directories', 'read-files', 'rename-files', 'replace', 'replica=', 'rest-api', 'service=', 'set-replica', 'summarize-errors', 'thredds', 'thredds-reinit', 'update', 'use-existing=', 'use-list=', 'validate=', 'version-list=', 'nodbwrite'])
    except getopt.error:
        print sys.exc_value
        return

    aggregateDimension = "time"
    datasetMapfile = None
    datasetName = None
    echoSql = False
    filefilt = '.*\.nc$'
    init_file = None
    initcontext = {}
    keepVersion = False
    las = False
    log_filename = None
    masterGateway = None
    message = None
    offline = False
    parent = None
    perVariable = None
    projectName = None
    properties = {}
    publish = False
    publishOnly = False
    publishOp = CREATE_OP
    readFiles = False
    rescan = False
    rescanDatasetName = []
    restApi = None
    schema = None
    service = None
    summarizeErrors = False
    testProgress1 = testProgress2 = None
    thredds = False
    threddsReinit = None
    version = None
    versionList = None
    nodbwrite = False

    for flag, arg in args:
        if flag=='-a':
            aggregateDimension = arg
        elif flag=='--append':
            publishOp = UPDATE_OP
        elif flag in ['-c', '--create']:
            publishOp = CREATE_OP
        elif flag=='--dataset':
            datasetName = arg
        elif flag in ['-d', '--delete-files']:
            publishOp = DELETE_OP
        elif flag=='--echo-sql':
            echoSql = True
        elif flag=='--experiment':
            initcontext['experiment'] = arg
        elif flag=='--filter':
            filefilt = arg
        elif flag in ['-h', '--help']:
            print usage
            sys.exit(0)
        elif flag=='-i':
            init_file = arg
        elif flag=='--keep-version':
            keepVersion = True
        elif flag=='--log':
            log_filename = arg
        elif flag=='--map':
            datasetMapfile = arg
        elif flag in ['-m', '--message']:
            message = arg
        elif flag=='--model':
            initcontext['model'] = arg
        elif flag=='--nodbwrite':
            nodbwrite = True
        elif flag=='--new-version':
            try:
                version = string.atoi(arg)
                if version <=0:
                    raise ValueError
            except ValueError:
                raise ESGPublishError("Version number must be a positive integer: %s"%arg)
        elif flag=='--no-thredds-reinit':
            threddsReinit = False
        elif flag=='--noscan':
            publishOnly = True
        elif flag=='--offline':
            offline = True
        elif flag=='--parent':
            parent = arg
        elif flag=='--per-time':
            perVariable = False
        elif flag=='--per-variable':
            perVariable = True
        elif flag=='--project':
            projectName = arg
        elif flag in ['-p', '--property']:
            name, value = arg.split('=')
            properties[name] = value
        elif flag=='--publish':
            publish = True
        elif flag in ['-e', '--read-directories']:
            readFiles = False
        elif flag=='--read-files':
            readFiles = True
        elif flag=='--rename-files':
            publishOp = RENAME_OP
        elif flag in ['-r', '--replace']:
            publishOp = REPLACE_OP
        elif flag=='--replica':
            masterGateway = arg
            warning("The --replica option is deprecated. Use --set-replica instead")
        elif flag=='--rest-api':
            restApi = True
        elif flag=='--service':
            service = arg
        elif flag=='--set-replica':
            masterGateway = 'DEFAULT'
        elif flag=='--summarize-errors':
            summarizeErrors = True
        elif flag=='--thredds':
            thredds = True
        elif flag=='--thredds-reinit':
            threddsReinit = True
        elif flag in ['-u', '--update']:
            publishOp = UPDATE_OP
        elif flag=='--use-existing':
            rescan = True
            rescanDatasetName.append(arg)
        elif flag=='--use-list':
            rescan = True
            if arg=='-':
                namelist=sys.stdin
            else:
                namelist = open(arg)
            for line in namelist.readlines():
                line = line.strip()
                if line[0]!='#':
                    rescanDatasetName.append(line)
        elif flag=='--validate':
            schema = arg
            restApi = True
        elif flag=='--version-list':
            versionList = arg

    # If offline, the project must be specified
    if offline and (projectName is None):
        raise ESGPublishError("Must specify project with --project for offline datasets")

    if version is not None and versionList is not None:
        raise ESGPublishError("Cannot specify both --new-version and --version-list")

    if versionList is not None:
        version = {}
        f = open(versionList)
        lines = f.readlines()
        f.close()
        for line in lines:
            line = line.strip()
            dsid, vers = line.split('|')
            dsid = dsid.strip()
            vers = int(vers.strip())
            version[dsid] = vers

    # Load the configuration and set up a database connection
    config = loadConfig(init_file)
    engine = create_engine(config.getdburl('extract'), echo=echoSql, pool_recycle=3600)
    initLogging('extract', override_sa=engine, log_filename=log_filename)
    Session = sessionmaker(bind=engine, autoflush=True, autocommit=False)

    # Register project handlers
    registerHandlers()

    # Get the default publication interface (REST or Hessian)
    if restApi is None:
        restApi = config.getboolean('DEFAULT', 'use_rest_api', default=False)

    # If the dataset map is input, just read it ...
    dmap = None
    directoryMap = None
    extraFields = None
    if datasetMapfile is not None:
        dmap, extraFields = readDatasetMap(datasetMapfile, parse_extra_fields=True)
        datasetNames = dmap.keys()

    elif rescan:
        # Note: No need to get the extra fields, such as mod_time, since
        # they are already in the database, and will be used for file comparison if necessary.
        dmap, offline = queryDatasetMap(rescanDatasetName, Session)
        datasetNames = dmap.keys()

    # ... otherwise generate the directory map.
    else:
        # Online dataset(s)
        if not offline:
            if len(lastargs)==0:
                print "No directories specified."
                return

            if projectName is not None:
                handler = getHandlerByName(projectName, None, Session)
            else:
                multiIter = multiDirectoryIterator(lastargs, filefilt=filefilt)
                firstFile, size = multiIter.next()
                listIter = list(multiIter)
                handler = getHandler(firstFile, Session, validate=True)
                if handler is None:
                    raise ESGPublishError("No project found in file %s, specify with --project."%firstFile)
                projectName = handler.name

            props = properties.copy()
            props.update(initcontext)
            if not readFiles:
                directoryMap = handler.generateDirectoryMap(lastargs, filefilt, initContext=props, datasetName=datasetName)
            else:
                directoryMap = handler.generateDirectoryMapFromFiles(lastargs, filefilt, initContext=props, datasetName=datasetName)
            datasetNames = [(item,-1) for item in directoryMap.keys()]

        # Offline dataset. Format the spec as a dataset map : dataset_name => [(path, size), (path, size), ...]
        else:
            handler = getHandlerByName(projectName, None, Session, offline=True)
            dmap = {}
            listerSection = getOfflineLister(config, "project:%s"%projectName, service)
            offlineLister = config.get(listerSection, 'offline_lister_executable')
            commandArgs = "--config-section %s "%listerSection
            commandArgs += " ".join(lastargs)
            for dsetName, filepath, sizet in processNodeMatchIterator(offlineLister, commandArgs, handler, filefilt=filefilt, datasetName=datasetName, offline=True):
                size, mtime = sizet
                if dmap.has_key((dsetName,-1)):
                    dmap[(dsetName,-1)].append((filepath, str(size)))
                else:
                    dmap[(dsetName,-1)] = [(filepath, str(size))]

            datasetNames = dmap.keys()

    datasetNames.sort()
    if len(datasetNames)==0:
        warning("No datasets found.")
        min_version = -1
    else:
        min_version = sorted(datasetNames, key=lambda x: x[1])[0][1]

    # Must specify version for replications
    if min_version == -1 and masterGateway is not None and version is None and versionList is None:
        raise ESGPublishError("Must specify version with --new-version (or --version-list) for replicated datasets")
    
    # Iterate over datasets
    if not publishOnly:

#        pdb.set_trace()

        datasets = iterateOverDatasets(projectName, dmap, directoryMap, datasetNames, Session, aggregateDimension, publishOp, filefilt, initcontext, offline, properties, keepVersion=keepVersion, newVersion=version, extraFields=extraFields, masterGateway=masterGateway, comment=message, readFiles=readFiles, nodbwrite=nodbwrite)


    if (not nodbwrite):
        result = publishDatasetList(datasetNames, Session, publish=publish, thredds=thredds, las=las, parentId=parent, service=service, perVariable=perVariable, reinitThredds=threddsReinit, restInterface=restApi, schema=schema)
    # print `result`

    if summarizeErrors:
        print 'Summary of errors:'
        for name,versionno in datasetNames:
            dset = Dataset.lookup(name, Session)
            print dset.get_name(Session), dset.get_project(Session), dset.get_model(Session), dset.get_experiment(Session), dset.get_run_name(Session)
            if dset.has_warnings(Session):
                print '=== Dataset: %s ==='%dset.name
                for line in dset.get_warnings(Session):
                    print line
