# flake8: noqa
from __future__ import print_function, division, absolute_import

import sys

PY2 = sys.version_info.major == 2
PY3 = not PY2

if PY2:
    from urlparse import urlparse
else:
    from urllib.parse import urlparse
