import sys
import os.path
import getopt
import logging
import string

import sqlalchemy

from pkg_resources import resource_filename
from sqlalchemy import create_engine, Table, MetaData
from sqlalchemy.orm import sessionmaker
from esgcet import __version__
from esgcet.model import *
from esgcet.config import loadConfig, splitLine, splitRecord, loadStandardNameTable, textTableIter, initLogging, loadModelsTable, initializeExperiments, SaneConfigParser
from esgcet.messaging import debug, info, warning, error, critical, exception
from migrate.versioning.api import version_control, upgrade, db_version, downgrade


def main(argv):

    try:
        args, lastargs = getopt.getopt(argv, "cd:hi:", ['db-version', 'echo-sql', 'version'])
    except getopt.error:
        print sys.exc_value
        return

    createTables = False
    dbVersion = False
    dgrade = False
    echoSql = None
    init_file = None
    for flag, arg in args:
        if flag=='-c':
            createTables = True
        elif flag=='-d':
            dgrade = True
            newVersion = string.atoi(arg)
        elif flag=='--db-version':
            dbVersion = True
        elif flag=='--echo-sql':
            echoSql = True
        elif flag=='-h':
            print usage
            sys.exit(0)
        elif flag=='-i':
            init_file = arg
        elif flag=='--version':
            print __version__
            return

    config = loadConfig(init_file)
    configOptions = config.options('initialize')
    
    dburl = config.getdburl('initialize')
    engine = create_engine(dburl, echo=echoSql, pool_recycle=3600)
    initLogging('initialize', override_sa=engine)

    if createTables or dbVersion or dgrade:
        repo_cfg_path = resource_filename('esgcet.schema_migration', 'migrate.cfg')
        repo_path = os.path.dirname(repo_cfg_path)

        # Verify that schema versioning is enabled
        try:
            version = db_version(dburl, repo_path, engine_dict={"echo": echoSql})
        except sqlalchemy.exc.NoSuchTableError:
            print 'Schema versioning not enabled. Run esgdrop_tables first.'
            sys.exit(0)

        # Ensure that the repository path is correctly recorded for the current version of Esgcet ...
        # ... Parse migrate.cfg for version_table and repository_id
        migrateConfig = SaneConfigParser(repo_cfg_path)
        migrateConfig.read(repo_cfg_path)
        versionTable = migrateConfig.get('db_settings', 'version_table')
        repositoryId = migrateConfig.get('db_settings', 'repository_id')

        # ... Reflect the verstion_table
        meta = MetaData(engine)
        migrationTable = Table(versionTable, meta, autoload=True)
        
        # ... Query the repository_path
        result = engine.execute(migrationTable.select(migrationTable.c.repository_id==repositoryId))
        resultTuple = list(result)[0]
        storedRepoPath = resultTuple['repository_path']

        # ... and correct it if necessary
        if storedRepoPath!=repo_path:
            info("Updating schema version directory to %s"%repo_path)
            updatestmt = migrationTable.update(
                whereclause="repository_id='%s'"%repositoryId,
                values={'repository_path':repo_path})
            updatestmt.execute()

    # Echo db version
    if dbVersion:
        version = db_version(dburl, repo_path, engine_dict={"echo": echoSql})
        print version
        return

    # Downgrade to older version
    if dgrade:
        ans = raw_input("Downgrade the database schema to version %d? (enter 'y' to continue): "%newVersion)
        if ans.lower()=='y':
            info("Downgrading schema to version %d"%newVersion)
            downgrade(dburl, repo_path, newVersion, engine_dict={"echo": echoSql})
        return

    # Upgrade to current version
    if createTables:
        info("Upgrading schema to latest version. (This may take a while.)")
        upgrade(dburl, repo_path, engine_dict={"echo": echoSql})

    Session = sessionmaker(bind=engine, autoflush=True, autocommit=False)
    session = Session()

    # Initialize standard name table
    info("Initializing standard names ...")
    path = config.get('initialize', 'initial_standard_name_table', default=None)
    for standardName in loadStandardNameTable(path):
        sname = session.query(StandardName).filter_by(name=standardName.name).first()
        if sname is None:
            session.add(standardName)

    # Initialize projects
    if 'project_options' in configOptions:
        info("Initializing projects, models, and experiments ...")
        projectOption = config.get('initialize', 'project_options')
        projectSpecs = splitRecord(projectOption)
        for projectName, projectDesc, search_order in projectSpecs:

            # First see if the project exists
            project = session.query(Project).filter_by(name=projectName).first()
            if project is None:
                project = Project(projectName, projectDesc)
                session.add(project)

            projectSection = 'project:'+projectName
            projectConfigOptions = config.options(projectSection)

            # Initialize models
            modelTable = config.get('initialize', 'initial_models_table', default=None)
            for lineno, modelTuple in loadModelsTable(modelTable):
                try:
                    projectId, modelName, modelUrl, modelDesc = modelTuple
                except:
                    raise ESGPublishError("Invalid line %d in %s: %s"%(lineno, modelTable, modelTuple))
                if projectId!=projectName:
                    continue

                # Check if the model exists
                model = session.query(Model).filter_by(name=modelName, project=projectName).first()
                if model is None:
                    model = Model(modelName, projectName, modelUrl, modelDesc)
                    project.models.append(model)
                    info("Adding model %s for project %s"%(modelName, projectName))
                    session.add(model)

            # Initialize experiments
            if 'experiment_options' in projectConfigOptions:
                initializeExperiments(config, projectName, session)

    session.commit()
    session.close()
