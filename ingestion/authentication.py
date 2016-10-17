import re
from OpenSSL import crypto
from pyasn1.codec.ber import decoder


import utils


"""
The util function checks if the extension in an X.509 certificate includes 'publisher' or 'admin' role for a specified group.
An example of the extension:

X509v3 extensions:
    1.2.3.4.4.3.2.1.7.8:
        u'esg.vo.group.roles=group_ACME_role_user;group_ACME_role_publisher:esg.vo.openid=https://esg.ccs.ornl.gov/esgf-idp/openid/lukasz'
"""

def is_publisher(openid, cert, group='.*', roles=['publisher', 'admin']):
    (server, username) = utils.decompose_openid(openid)
    ext_count = cert.get_extension_count()
    for i in range(0, ext_count):
        ext = cert.get_extension(i)
        asn1data = ext.get_data()
        utf8string = decoder.decode(asn1data)[0].prettyPrint()
        pairs = utf8string.split(':', 1)
        if len(pairs) < 1:
            continue
        if not re.match('esg.vo.openid=%s$' % openid, pairs[1]):
            continue
        pv = pairs[0].split('=')
        if len(pv) < 2:
            continue
        if not re.match('esg.vo.group.roles$', pv[0]):
            continue
        cert_roles = None
        cert_roles = pv[1].split(';')
        for cr in cert_roles:
            for r in roles:
                regex = 'group_%s_role_%s$' % (group, r)
                if re.match(regex, cr):
                    return True
    return False
