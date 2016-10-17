import os
import urlparse


def decompose_openid(openid):
    url_decomposed = urlparse.urlparse(openid)
    username = os.path.basename(url_decomposed.path)
    server = url_decomposed.netloc
    return (server, username)
