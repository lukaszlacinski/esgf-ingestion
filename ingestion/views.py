import os
import json
import subprocess
import urlparse
from myproxy.client import MyProxyClient
from OpenSSL import crypto
from pyasn1.type import useful
from datetime import datetime

from pyramid.httpexceptions import (
    HTTPFound,
    HTTPNotFound,
)

from pyramid.response import Response
from pyramid.view import (
    view_config,
    forbidden_view_config,
    )

from pyramid.security import (
    remember,
    forget,
    authenticated_userid,
    )

from sqlalchemy.exc import DBAPIError

from .models import (
    DBSession,
    Publisher,
    Submission,
    SubmissionMetadata,
    SubmissionFacet,
    FacetName,
    FacetValue,
    )

import utils
import authentication

from celery import result

from backend import (
    celery_app,
    transfer,
    scan,
    publish,
    )


@view_config(route_name='home', renderer='json', permission='view')
def home(request):
    return Response('ESGF Ingestion REST API\n')


@view_config(route_name='authenticate', renderer='json', permission='view')
def authenticate(request):
    if request.method != 'POST':
        return Response('Error: GET is not supported')

    data = json.loads(request.body.decode('utf-8'))

    openid = data.get('openid')
    password = data.get('password')

    (server, username) = utils.decompose_openid(openid)

    # Get X.509 certificate chain from MyProxy server
    print "Getting X.509 certificate from %s for %s" % (server, username)
    myproxy_client = MyProxyClient(hostname=server)
    cred_chain_pem_tuple = None
    try:
        cred_chain_pem_tuple = myproxy_client.logon(username, password, lifetime=7*24*3600)
    except Exception as e:
        request.response.status = 400
        return {'status': 'Error', 'message': '%s' % e}

    cred_chain_pem = ''
    for e in cred_chain_pem_tuple:
        cred_chain_pem += e
    cert_pem = cred_chain_pem_tuple[0]

    # Get 'Not After' date
    cert = crypto.load_certificate(crypto.FILETYPE_PEM, cert_pem)
    not_after_asn1 = cert.get_notAfter()
    not_after = not_after_asn1.decode()
    dt = datetime.strptime(not_after, '%Y%m%d%H%M%SZ')

    # Check the publisher role in X509v3 extension 1.2.3.4.4.3.2.1.7.8
    if not authentication.is_publisher(openid, cert):
        request.response.status = 400
        print 'DUPA!!!!'
        return {'status': 'Error', 'message': 'The user does not have the publisher role'}


    # Store the X.509 certificate chain in a tmp file, so it can be used later by esgcet
    cred_file = open('/tmp/x509in_%s_%s' % (server, username), 'w')
    cred_file.write(cred_chain_pem)
    cred_file.close()

    # Add or update Publisher object in the database
    publisher = DBSession.query(Publisher).filter(Publisher.openid==openid).first()
    if publisher:
        publisher.x509_pem = cred_chain_pem
        publisher.expiration = dt
    else:
        publisher = Publisher(openid=openid, x509_pem=cred_chain_pem, expiration=dt)
        DBSession.add(publisher)

    # Save openid in auth_tk cookie
    headers = remember(request, openid)
    resp = Response()
    resp.headers = headers
    return resp


@view_config(route_name='workflow_create', renderer='json', permission='publish')
def workflow_create(request):
    if request.method != 'POST':
        return Response('Error:')

    openid = authenticated_userid(request)
    publisher = DBSession.query(Publisher).filter(Publisher.openid==openid).first()
    data = json.loads(request.body.decode('utf-8'))

    print('openid: %s' % openid)
    print('data: %s' % data)

    submission = Submission()

    metadata = data.get('metadata')
    if metadata:
        for md in metadata:
            name = md.get('name')
            value = md.get('value')
            if name is not None and value is not None:
                submission.submission_metadata.append(SubmissionMetadata(name=name, value=value))

    facets_data = data.get('facets')
    if facets_data:
        submission_facets = {}
        for f in facets_data:
            fname = f.get('name')
            fvalue = f.get('value')
            submission_facets[fname] = fvalue

    project = submission_facets['project']
    if project is None:
        return {'status': 'Error', 'message': 'Missing the "project" facet'}

    cert = crypto.load_certificate(crypto.FILETYPE_PEM, publisher.x509_pem)
    if not authentication.is_publisher(openid, cert, group=project, roles=['publisher']):
        return {'Error': '%s does not have the publisher role in the %s project' % (openid, project)}


    for fname in submission_facets:
        fvalue = submission_facets[fname]
        facet_name = DBSession.query(FacetName).filter(FacetName.name==fname).first()
        if facet_name is None:
            return { 'status': 'Error', 'message': "Unknown facet name: '%s'" % fname }
        facet_name = DBSession.query(FacetName).filter(FacetName.name==fname).first()
        facet_value = DBSession.query(FacetValue).filter(FacetValue.name_id==facet_name.id).filter(FacetValue.value==fvalue).first()
        if facet_value is None:
            facet_value = FacetValue(facet_name=facet_name, value=fvalue)
        sfacet = SubmissionFacet()
        facet_value.submission_facet.append(sfacet)
        submission.submission_facet.append(sfacet)

    return {'status': 'Success', 'submission_id': submission.id, 'message': 'The publication workflow has been created successfully'}


@view_config(route_name='workflow_transfer', renderer='json', permission='publish')
def workflow_transfer(request):

    openid = request.authenticated_userid
    submission_id = request.matchdict['workflow_id']
    if request.method != 'POST':
        return HttpResponse('Error:')
    data = json.loads(request.body.decode('utf-8'))

    endpoint = data.get('endpoint')
    path = data.get('path')
    access_token = data.get('access_token')

    submission = DBSession.query(Submission).filter(Submission.id==submission_id).first()
    if submission is None:
        return {'status': 'Error', 'message': 'Wrong submission id: %s' % submission_id }

    if submission.task_id is not None:
        ar = result.AsyncResult(id=submission.task_id, app=celery_app)
        if ar.state != 'SUCCESS' and ar.state != 'FAILURE':
            return {'status': 'Running', 'message': 'Another task, %s, is still running' % submission.task_name }

    metadata = DBSession.query(SubmissionMetadata).\
            filter(SubmissionMetadata.submission_id==submission_id).\
            filter(SubmissionMetadata.name=='datanode').first()


    ar = transfer.delay(openid=openid, datanode=metadata.value, submission_id='%s' % submission.id)
    submission.task_id = ar.id
    submission.task_name = ar.task_name
    if path:
        submission.path = path

    return {'status': 'Success', 'message': 'Scan task %s started successfully' % ar.id }


@view_config(route_name='workflow_scan', renderer='json', permission='publish')
def workflow_scan(request):

    openid = request.authenticated_userid
    submission_id = request.matchdict['workflow_id']
    if request.method != 'POST':
        return HttpResponse('Error:')
    data = json.loads(request.body.decode('utf-8'))

    path = data.get('path')


    submission = DBSession.query(Submission).filter(Submission.id==submission_id).first()
    if submission is None:
        return {'status': 'Error', 'message': 'Wrong submission id: %s' % submission_id }

    if submission.task_id is not None:
        ar = result.AsyncResult(id=submission.task_id, app=celery_app)
        if ar.state != 'SUCCESS' and ar.state != 'FAILURE':
            return {'status': 'Running', 'message': 'Another task, %s, is still running' % submission.task_name }

    metadata = DBSession.query(SubmissionMetadata).\
            filter(SubmissionMetadata.submission_id==submission_id).\
            filter(SubmissionMetadata.name=='datanode').first()

    facets = DBSession.query(FacetName, FacetValue, SubmissionFacet).\
            filter(FacetName.id==FacetValue.name_id).\
            filter(FacetValue.id==SubmissionFacet.value_id).\
            filter(SubmissionFacet.submission_id==submission_id).order_by(FacetName.id).all()

    # Start a scan process for the specified workflow and facets
    fnv = []
    for fname, fvalue, sfacet in facets:
        fnv.append({ 'name': fname.name, 'value': fvalue.value })
        print('%d: %s:%s' % (submission.id, fname.name, fvalue.value))

    ar = scan.delay(openid=openid, submission_id='%s' % submission.id, facets=fnv, path=path)
    submission.task_id = ar.id
    submission.task_name = ar.task_name
    if path:
        submission.path = path

    return {'status': 'Success', 'message': 'Scan task %s started successfully' % ar.id }


@view_config(route_name='workflow_publish', renderer='json', permission='publish')
def workflow_publish(request):

    openid = request.authenticated_userid
    (server, username) = utils.decompose_openid(openid)

    submission_id = request.matchdict['workflow_id']

    submission = DBSession.query(Submission).filter(Submission.id==submission_id).first()
    if submission is None:
        return {'status': 'Error', 'message': 'Wrong submission id: %s' % submission_id }

    if submission.task_id is not None:
        ar = result.AsyncResult(id=submission.task_id, app=celery_app)
        if ar.state != 'SUCCESS' and ar.state != 'FAILURE':
            return {'status': 'Running', 'message': 'Another task, %s, is still running' % submission.task_name }

    metadata = DBSession.query(SubmissionMetadata).\
            filter(SubmissionMetadata.submission_id==submission_id).\
            filter(SubmissionMetadata.name=='datanode').first()

    ar = publish.delay(openid=openid, datanode=metadata.value, submission_id='%s' % submission.id, path=submission.path)
    submission.task_id = ar.id
    submission.task_name = ar.task_name

    return {'status': 'Success', 'message': 'Publish task %s started successfully' % ar.id }


@view_config(route_name='workflow_status', renderer='json', permission='publish')
def workflow_status(request):
    submission_id = request.matchdict['workflow_id']

    submission = DBSession.query(Submission).filter(Submission.id==submission_id).first()
    if submission is None:
        return {'status': 'Error', 'message': 'Wrong submission id: %s' % submission_id }

    if submission.task_id is None:
        return { 'status': 'Error', 'message': 'No task has been submitted yet' }

    ar = result.AsyncResult(id=submission.task_id, app=celery_app)
    if ar.state != 'SUCCESS' and ar.state != 'FAILURE':
        return {'status': 'Running', 'message': 'Task %s is still running' % submission.task_name }

    r = ar.get()
    return r


