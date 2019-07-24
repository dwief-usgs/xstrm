
#import pkg_resources  # part of setuptools


# Import bis objects
from . import build_network
from . import build_network_rpu
from . import network_calc


# provide version, PEP - three components ("major.minor.micro")
#__version__ = pkg_resources.require("xstrm")[0].version


# metadata retrieval
#def get_package_metadata():
#    d = pkg_resources.get_distribution('xstrm')
#    for i in d._get_metadata(d.PKG_INFO):
#        print(i)
