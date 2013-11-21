from distutils.core import setup
from distutils.errors import DistutilsSetupError

AUTHOR_INFO = [
    ('Paolo Anedda', 'paolo.anedda@crs4.it'),
    ('Luca Lianas', 'luca.lianas@crs4.it')
]
MAINTAINER_INFO = AUTHOR_INFO
AUTHOR = '', ''.join(t[0] for t in AUTHOR_INFO)
AUTHOR_EMAIL = ", ".join("<%s>" % t[1] for t in AUTHOR_INFO)
MAINTAINER = ", ".join(t[0] for t in MAINTAINER_INFO)
MAINTAINER_EMAIL = ", ".join("<%s>" % t[1] for t in MAINTAINER_INFO)

try:
    with open("NAME") as f:
        NAME = f.read().strip()
    with open("VERSION") as f:
        VERSION = f.read().strip()
except IOError:
    raise DistutilsSetupError("failed to read name/version info")

setup(
    name=NAME,
    version=VERSION,
    author=AUTHOR,
    author_email=AUTHOR_EMAIL,
    maintainer=MAINTAINER,
    maintainer_email=MAINTAINER_EMAIL,
    packages=[
        'openehr',
        'openehr.utils',
        'openehr.archetypes',
        'openehr.archetypes.archive',
        'openehr.archetypes.archive.drivers',
        'openehr.aql'
    ],
)
