import os
from flask import Flask, url_for, send_from_directory, redirect


class PrefixMiddleware(object):

    def __init__(self, app, prefix=''):
        self.app = app
        self.prefix = prefix

    def __call__(self, environ, start_response):
        if environ['PATH_INFO'].startswith(self.prefix):
            environ['PATH_INFO'] = environ['PATH_INFO'][len(self.prefix):]
            environ['SCRIPT_NAME'] = self.prefix
            return self.app(environ, start_response)
        else:
            start_response('404', [('Content-Type', 'text/plain')])
            return [str(self.prefix + ". This url does not belong to the app.").encode()]


def create_app(test_config=None):
    prefix = "/"
    if "APP_PREFIX" in os.environ:
        prefix = os.environ['APP_PREFIX']
    # create and configure the app
    app = Flask(__name__, instance_relative_config=False)
    app.config["APPLICATION_ROOT"] = prefix
    app.debug = True

    # app.wsgi_app = PrefixMiddleware(app.wsgi_app, prefix=prefix)

    app.config.from_mapping(
        SECRET_KEY='dev',
        DATABASE=os.path.join(app.instance_path, 'ontario.sqlite')
    )

    if test_config is None:
        # load the instance config, if it exists, when not testing
        app.config.from_pyfile('config.py', silent=True)
    else:
        # load the test config if passed in
        app.config.from_mapping(test_config)

    # ensure the instance folder exists
    try:
        os.makedirs(app.instance_path)
    except OSError:
        pass

    from . import query
    app.register_blueprint(query.bp)

    return app