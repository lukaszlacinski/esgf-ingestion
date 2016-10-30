from __future__ import absolute_import

from celery import Celery


celery_app = Celery('tasks', backend='db+postgres://ingestion:<password>@localhost/esgf_ingestion_celery',
        broker='amqp://guest:guest@localhost:5672//',
        )

celery_app.conf.update(
#        CELERY_TASK_SERIALIZER = 'json',
#        CELERY_RESULT_SERIALIZER = 'json',
#        CELERY_ACCEPT_CONTENT=['json'],
        CELERY_TIMEZONE = 'America/Chicago',
        CELERY_ENABLE_UTC = True
        )


import os
import time
import urlparse
import shutil
from string import Template
import subprocess
from subprocess import (
        Popen,
        PIPE,
        )

from backend import esgscan_directory
from backend import esginitialize
from backend import esgpublish
from backend import esgunpublish

from globusonline.transfer.api_client import Transfer, TransferAPIClient


dataroot_path = '/esg/data_ingestion'
submission_path = '/esg/gridftp_root/ingestion'
tmp_config_path = '/esg/tmp/ingestion'

@celery_app.task
def transfer(src_endpoint, src_path, dst_endpoint, dst_path, access_token, openid):
    (server, username) = decompose_openid(openid)
    client = TransferAPIClient(username, goauth=access_token)
    client.connect()
    globus_submission_id = client.submission_id()
    transfer = Transfer(globus_submission_id, src_endpoint, dst_endpoint)
    transfer.add_item(src_path, dst_path, recursive=True)
    client.transfer(transfer)
    return {'status': 'Success', 'message': ''}


def create_esgini(path, submission_id, facets, dataset_root=None):
    esgini = open(path, 'w')

    esg_pre = open('/esg/config/esgcet/esg.ini_pre')
    esg_post = open('/esg/config/esgcet/esg.ini_post')

    src = Template(esg_pre.read())
    result = src.substitute({'itemid': submission_id})
    esgini.write(result)

    esgini.write('[project:%s]\n' % facets[0]['value'])
    esgini.write('categories =\n')
    i = 0
    for f in facets:
        esgini.write('    %s | enum | true | true | %d\n' % (f['name'], ++i))
    for f in facets:
        if f['name'] == 'experiment':
            esgini.write('experiment_options = %s | %s | experiment %s\n' % (facets[0]['value'], f['value'], f['value']))
        else:
            esgini.write('%s_options = %s\n' % (f['name'], f['value']))
    esgini.write('directory_format = ')
    if dataset_root:
        esgini.write('%s/' % dataset_root)
    for f in facets:
        if f['name'] == 'project':
            esgini.write('%%(%s)s' % f['name'])
        else:
            esgini.write('/%%(%s)s' % f['name'])
    esgini.write('\n')
    esgini.write('dataset_id = ')
    for f in facets:
        if f['name'] == 'project':
            esgini.write('%%(%s)s' % f['name'])
        else:
            esgini.write('.%%(%s)s' % f['name'])
    esgini.write('\n')

    esgini.write(esg_post.read())
    esgini.close()


@celery_app.task
def scan(openid, submission_id, facets, path=None, x509=None, username=None):

    # Move all files to tmp/, create a facet directory sequence, move all files from tmp/ to the directory sequence.
    if path:
        path = os.path.join(submission_path, path.lstrip('/'))
    else:
        path = os.path.join(submission_path, 'submission_%s' % submission_id)
    tmpdir = os.path.join(path, 'tmp')
    try:
        os.makedirs(tmpdir)
    except Exception as e:
        pass

    # Move all files to tmp/
    for dpath, dname, fname in os.walk(path):
        if dpath != tmpdir:
            for f in fname:
                os.rename(os.path.join(dpath, f), os.path.join(tmpdir, f))

    # Delete all directories besides tmp/
    for f in os.listdir(path):
        if f == 'tmp':
            break
        fpath = os.path.join(path, f)
        if os.path.isfile(fpath):
            os.remove(fpath)
        else:
            shutil.rmtree(fpath)

    # Create a facet directory sequence
    facetdir = path
    for f in facets:
        facetdir = os.path.join(facetdir, f['value'])
    try:
        os.makedirs(facetdir)
    except Exception as e:
        pass

    # Move all files from tmp/
    for f in os.listdir(tmpdir):
        os.rename(os.path.join(tmpdir, f), os.path.join(facetdir, f))
    os.rmdir(tmpdir)

    # Create esg.ini files in /esg/tmp/ingestion/
    esginiscanpath = os.path.join(tmp_config_path, 'esg.ini_%s_scan' % submission_id)
    esginipath = os.path.join(tmp_config_path, 'esg.ini_%s' % submission_id)
    create_esgini(esginiscanpath, submission_id, facets)
    create_esgini(esginipath, submission_id, facets, path)

    # Scan all dataset files in submission_<submission_id>/
    os.environ['ESGINI'] = esginiscanpath
    os.chdir(path)
    mapscanpath = os.path.join(tmp_config_path, '%s_map.txt_scan' % submission_id)
    mappath = os.path.join(tmp_config_path, '%s_map.txt' % submission_id)
    argv = ['--project', facets[0]['value'], '-o', mapscanpath, facets[0]['value']]
    esgscan_directory.main(argv)

    files = []
    mapfile = open(mappath, 'w')
    with open(mapscanpath) as f:
        for line in f:
            e = line.split(' | ')
            if len(e) >= 2:
                 f = os.path.basename(e[1])
                 files += [ f ]
            mapfile.write(line.replace('| ', '| %s/' % dataroot_path, 1))
    mapfile.close()

    return {'status': 'Success', 'message': '', 'files': files }


def create_links(src_path, dst_path):
    for i in os.listdir(src_path):
        if os.path.isdir(os.path.join(src_path, i)):
            try:
                os.mkdir(os.path.join(dst_path, i))
            except Exception as e:
                pass
            create_links(os.path.join(src_path, i), os.path.join(dst_path, i))
        elif os.path.isfile(os.path.join(src_path, i)):
            try:
                os.unlink(os.path.join(dst_path, i))
            except Exception as e:
                pass
            try:
                os.link(os.path.join(src_path, i), os.path.join(dst_path, i))
            except Exception as e:
                print os.path.join(src_path, i), os.path.join(dst_path, i)
                return


@celery_app.task
def publish(openid, datanode, submission_id, x509=None, username=None, path=None):
    user_cred_path = '/tmp/x509in_%s_%s' % decompose_openid(openid)
    os.environ['X509_USER_PROXY'] = user_cred_path
    mappath = os.path.join(tmp_config_path, '%s_map.txt' % submission_id)

    if path:
        path = os.path.join(submission_path, path.lstrip('/'))
    else:
        path = os.path.join(submission_path, 'submission_%s' % submission_id)

    # Link data_ingestion/ to ingestion/submission_<submission_id>/
    create_links(path, dataroot_path)

    # Generate THREDDS catalog
    esginipath = os.path.join(tmp_config_path, 'esg.ini_%s' % submission_id)
    os.environ['ESGINI'] = esginipath
    argv = ['-c']
    esginitialize.main(argv)

    argv = ['--project', 'ACME', '--thredds', '--service', 'fileservice', '--map', mappath]
    esgpublish.main(argv)

    # Push metadata to Solr
    shutil.copyfile(user_cred_path, os.path.join(tmp_config_path, 'certificate-file_%s' % submission_id))
    argv = ['--project', 'ACME', '--publish', '--map', mappath]
    esgpublish.main(argv)

    return {'status': 'Success', 'message': '' }


def decompose_openid(openid):
    url_decomposed = urlparse.urlparse(openid)
    username = os.path.basename(url_decomposed.path)
    server = url_decomposed.netloc
    return (server, username)
