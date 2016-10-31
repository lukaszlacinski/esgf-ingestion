from pyramid.config import Configurator
from pyramid.authentication import AuthTktAuthenticationPolicy
from pyramid.authorization import ACLAuthorizationPolicy
from sqlalchemy import engine_from_config


from .models import (
    DBSession,
    Base,
    RootFactory,
    get_principals,
    )


def main(global_config, **settings):
    """ This function returns a Pyramid WSGI application.
    """
    engine = engine_from_config(settings, 'sqlalchemy.')
    DBSession.configure(bind=engine)
    Base.metadata.bind = engine

    authn_policy = AuthTktAuthenticationPolicy('seekrit', callback=get_principals, hashalg='sha512')
    authz_policy = ACLAuthorizationPolicy()

    config = Configurator(
            settings=settings,
            root_factory=RootFactory,
            authentication_policy=authn_policy,
            authorization_policy=authz_policy
    )

    config.add_static_view('static', 'static', cache_max_age=3600)
    config.add_route('home', '/')
    config.add_route('authenticate', '/auth')
    config.add_route('workflow_create', '/workflow/create')
    config.add_route('workflow_transfer', '/workflow/create')
    config.add_route('workflow_scan', '/workflow/{workflow_id}/scan')
    config.add_route('workflow_publish', '/workflow/{workflow_id}/publish')
    config.add_route('workflow_status', '/workflow/{workflow_id}/status')
    config.scan()

    return config.make_wsgi_app()
