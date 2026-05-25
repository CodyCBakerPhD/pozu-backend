import sys

project_home = "/home/CodyCBakerPhD/mysite/pozu-backend"
if project_home not in sys.path:
    sys.path = [project_home] + sys.path

# import flask app but need to call it "application" for WSGI to work
from pozu_flask_app import app as application  # noqa
