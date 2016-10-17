import datetime

from pyramid.security import (
    Allow,
    Everyone,
    Authenticated,
    )

from sqlalchemy import (
    Column,
    Index,
    Integer,
    String,
    Text,
    DateTime,
    ForeignKey,
    UniqueConstraint,
    )

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, backref

from sqlalchemy.orm import (
    scoped_session,
    sessionmaker,
    )

from zope.sqlalchemy import ZopeTransactionExtension

DBSession = scoped_session(sessionmaker(extension=ZopeTransactionExtension()))
Base = declarative_base()


class RootFactory(object):
    __acl__ = [ (Allow, Everyone, 'view'),
                (Allow, Authenticated, 'publish') ]

    def __init__(self, request):
        pass


def get_principals(userid, request):
    if userid:
        return [Everyone, Authenticated]
    return [Everyone]


class Publisher(Base):
    __tablename__ = 'publisher'
    id = Column(Integer, primary_key=True)
    openid = Column(String(1024), unique=True)
    x509_pem = Column(Text)
    expiration = Column(DateTime)


class FacetName(Base):
    __tablename__ = 'facet_name'
    id = Column(Integer, primary_key=True)
    name = Column(String(255), unique=True)

    facet_value = relationship("FacetValue", backref=backref('facet_name'))


class FacetValue(Base):
    __tablename__ = 'facet_value'
    id = Column(Integer, primary_key=True)
    name_id = Column(Integer, ForeignKey('facet_name.id'))
    value = Column(String(255))

    UniqueConstraint(name_id, value)

    submission_facet = relationship("SubmissionFacet", backref=backref('facet_value'))


class Submission(Base):
    __tablename__ = 'submission'
    id = Column(Integer, primary_key=True)
    date = Column(DateTime, default=datetime.datetime.utcnow)
    path = Column(String(255))
    task_id = Column(String(255))
    task_name = Column(String(255))
    status = Column(String(255))
    message = Column(String(10240))

    submission_metadata = relationship("SubmissionMetadata", backref=backref('submission'))
    submission_facet = relationship("SubmissionFacet", backref=backref('submission'))
    dataset_file = relationship("DatasetFile", backref=backref('submission'))


class SubmissionMetadata(Base):
    __tablename__ = 'submission_metadata'
    id = Column(Integer, primary_key=True)
    submission_id = Column(Integer, ForeignKey('submission.id'))
    name = Column(String(255))
    value = Column(String(1023))


class SubmissionFacet(Base):
    __tablename__ = 'submission_facet'
    id = Column(Integer, primary_key=True)
    submission_id = Column(Integer, ForeignKey('submission.id'))
    value_id = Column(Integer, ForeignKey('facet_value.id'))


class DatasetFile(Base):
    __tablename__ = 'dataset_file'
    id = Column(Integer, primary_key=True)
    submission_id = Column(Integer, ForeignKey('submission.id'))
    filename = Column(String(1023))
