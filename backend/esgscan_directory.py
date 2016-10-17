import sys
import os
import re
import getopt

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from esgcet.publish import multiDirectoryIterator, directoryIterator, processIterator, readDatasetMap, processNodeMatchIterator, checksum, fnIterator
from esgcet.config import loadConfig, getHandler, getHandlerByName, initLogging, registerHandlers, getOfflineLister, splitLine
from esgcet.exceptions import *
from esgcet.messaging import debug, info, warning, error, critical, exception
from multiprocessing.pool import ThreadPool


def main(argv):
    try:
        args, lastargs = getopt.getopt(argv, "a:ehi:o:p:", ['dataset=', 'dataset-tech-notes=', 'dataset-tech-notes-title=',\
            'filter=', 'help', 'max-threads=', 'offline', 'output=', 'project=', 'property=', 'read-directories', 'read-files',\
            'service=', 'use-version-dir', 'version='])
    except getopt.error:
        print sys.exc_value
        return

    if len(lastargs)==0:
        print 'No directory specified'
        return

    appendMap = None
    datasetName = None
    datasetTechNotesURL = None
    datasetTechNotesTitle = None
    filefilt = '.*\.nc$'
    init_file = None
    offline = False
    output = sys.stdout
    projectName = None
    properties = {}
    readFiles = False
    service = None
    max_threads = 4
    version_dir = False
    use_version = None
    
    for flag, arg in args:
        if flag=='-a':
            if os.path.exists(arg):
                appendMap = readDatasetMap(arg)
            else:
                appendMap = {}
            output = open(arg, 'a')
        elif flag=='--dataset':
            datasetName = arg
        elif flag=='--dataset-tech-notes':
            datasetTechNotesURL = arg
        elif flag=='--dataset-tech-notes-title':
            datasetTechNotesTitle = arg
        elif flag=='--filter':
            filefilt = arg
        elif flag in ['-h', '--help']:
            print usage
            sys.exit(0)
        elif flag=='-i':
            init_file = arg
        elif flag=='--max-threads':
            max_threads = int(arg)
        elif flag in ['-o', '--output']:
            output = open(arg, 'w')
        elif flag=='--offline':
            offline = True
        elif flag=='--project':
            projectName = arg
        elif flag in ['-p', '--property']:
            name, value = arg.split('=')
            properties[name] = value
        elif flag in ['-e', '--read-directories']:
            readFiles = False
        elif flag=='--read-files':
            readFiles = True
        elif flag=='--service':
            service = arg
        elif flag=='--use-version-dir':
            version_dir = True
        elif flag=='--version':
            version_dir = True
            if not re.match('^[0-9]+$', arg[0]): # e.g. 'vYYYYMMDD'
                use_version = arg[1:]
            else:
                use_version = arg
    
    # Load the configuration and set up a database connection
    config = loadConfig(init_file)
    engine = create_engine(config.getdburl('extract'), echo=False, pool_recycle=3600)
    initLogging('extract', override_sa=engine)
    Session = sessionmaker(bind=engine, autoflush=True, autocommit=False)

    # Register project handlers
    registerHandlers()

    if not offline:

        # Determine if checksumming is enabled
        line = config.get('DEFAULT', 'checksum', default=None)
        if line is not None:
            checksumClient, checksumType = splitLine(line)
        else:
            checksumClient = None

        if projectName is not None:
            handler = getHandlerByName(projectName, None, Session)
        else:
            warning("No project name specified!")
            multiIter = multiDirectoryIterator(lastargs, filefilt=filefilt)
            firstFile, size = multiIter.next()
            handler = getHandler(firstFile, Session, validate=True)
            if handler is None:
                raise ESGPublishError("No project found in file %s, specify with --project."%firstFile)
            projectName = handler.name

        if not readFiles:
            datasetMap = handler.generateDirectoryMap(lastargs, filefilt, initContext=properties, datasetName=datasetName, use_version=version_dir)
        else:
            datasetMap = handler.generateDirectoryMapFromFiles(lastargs, filefilt, initContext=properties, datasetName=datasetName)

        # Output the map
        keys = datasetMap.keys()
        keys.sort()

        datasetMapVersion = {}
        if version_dir:
            # check for version directory
            for dataset_id in keys:
                ds_id_version = dataset_id.split('#')
                if len(ds_id_version) == 2:
                    ds_id, ds_version = ds_id_version
                    if not re.match('^[0-9]+$', ds_version):
                        warning("Version must be an integer. Skipping version %s of dataset %s."%(ds_version, ds_id))
                        continue
                    if use_version and ds_version != use_version:
                            continue
                    if ds_id in datasetMapVersion:
                        datasetMapVersion[ds_id].append(ds_version)
                    else:
                        datasetMapVersion[ds_id] = [ds_version]
                else:
                    error("No version directory found. Skipping dataset %s."%dataset_id)

            if datasetMapVersion:
                keys = datasetMapVersion.keys()
                keys.sort()
            else:
                if use_version:
                    error("Version %s not found. No datasets to process."%use_version)
                else:
                    error("No datasets to process.")
                return

        for dataset_id in keys:
            skip_dataset = False
            dataset_id_version = dataset_id
            path_version = None
            # if multiple versions of the same dataset available use latest version
            if version_dir:
                path_version = sorted(datasetMapVersion[dataset_id])[-1]
                if len(datasetMapVersion[dataset_id]) > 1:
                    info("Multiple versions for %s (%s), processing latest (%s)"%(dataset_id, datasetMapVersion[dataset_id], path_version))
                dataset_id_version = '%s#%s'%(dataset_id, path_version)

            direcTuple = datasetMap[dataset_id_version]
            direcTuple.sort()
            mapfile_md = {}

            for nodepath, filepath in direcTuple:

                # If readFiles is not set, generate a map entry for each file in the directory
                # that matches filefilt ...
                if not readFiles:
                    itr = directoryIterator(nodepath, filefilt=filefilt, followSubdirectories=False)
                # ... otherwise if readFiles is set, generate a map entry for each file
                else:
                    itr = fnIterator([filepath])

                for filepath, sizet in itr:
                    size, mtime = sizet

                    mapfile_md[filepath] = [size]
                    mapfile_md[filepath].append("mod_time=%f"%float(mtime))

                    extraStuff = "mod_time=%f"%float(mtime)

                    if datasetTechNotesURL is not None:
                        mapfile_md[filepath].append('dataset_tech_notes=%s'%datasetTechNotesURL)
                        if datasetTechNotesURL is not None:
                            mapfile_md[filepath].append('dataset_tech_notes_title=%s'%datasetTechNotesTitle)

            if checksumClient is not None:
                pool = ThreadPool(processes=max_threads)
                args = [(filepath, checksumClient) for filepath in mapfile_md]
                checksum_list = pool.map(calc_checksum_wrapper, args)

                for entry in checksum_list:
                    if not entry[1]:
                        error('Calculation of checksum for file %s failed. Skipping dataset %s ...'%(entry[0], dataset_id))
                        skip_dataset = True     # skip entire dataset if we have one file without checksum
                        break
                    mapfile_md[entry[0]].append('checksum=%s'%entry[1])
                    mapfile_md[entry[0]].append('checksum_type=%s'%checksumType)

            for fpath in mapfile_md:
                mapfile_line = '%s | %s | %d'%(dataset_id_version, fpath, mapfile_md[fpath][0])

                for md in mapfile_md[fpath][1:]:
                    mapfile_line+=' | %s'%md

                # Print the map entry if:
                # - Checksum exists for all files of dataset (in case checksumming is enabled)
                # - The map is being created, not appended, or
                # - The existing map does not have the dataset, or
                # - The existing map has the dataset, but not the file.
                if path_version:
                    ds_id = (dataset_id, int(path_version))
                else:
                    ds_id = (dataset_id, -1)
                if not skip_dataset and ( (appendMap is None) or (not appendMap.has_key(ds_id)) or (( fpath, "%d"% mapfile_md[fpath][1]) not in appendMap[ds_id]) ):
                    print >>output, mapfile_line

    else:                               # offline
        if projectName is not None:
            handler = getHandlerByName(projectName, None, Session, offline=True)
        else:
            raise ESGPublishError("Must specify --project for offline datasets.")
        listerSection = getOfflineLister(config, "project:%s"%projectName, service)
        offlineLister = config.get(listerSection, 'offline_lister_executable')
        commandArgs = "--config-section %s "%listerSection
        commandArgs += " ".join(lastargs)
        for dsetName, filepath, sizet in processNodeMatchIterator(offlineLister, commandArgs, handler, filefilt=filefilt, datasetName=datasetName, offline=True):
            size, mtime = sizet
            extrastuff = ""
            if mtime is not None:
                extrastuff = "| mod_time=%f"%float(mtime)
            if (appendMap is None) or (not appendMap.has_key(dsetName)) or ((filepath, "%d"%size) not in appendMap[dsetName]):
                print >>output, "%s | %s | %d %s"%(dsetName, filepath, size, extrastuff)

    if output is not sys.stdout:
        output.close()

def calc_checksum_wrapper(args):
    return calc_checksum(*args)

def calc_checksum(filepath, checksum_client):
    csum = None
    if os.path.exists(filepath):
        command = "%s %s"%(checksum_client, filepath)
        info("Running: %s"%command)
        try:
            f = os.popen(command).read()
            csum = f.split(' ')[0]
        except:
            error("Error running command '%s', check configuration option 'checksum'."%command)

    return filepath, csum
