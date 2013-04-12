from distutils.core import setup
from os.path import isfile, join, exists
import platform
import glob
import os

# common paths across platforms
# version must be set as a string
# other scripts look for """'version' = """ in this file.
version = "1.0.1" 
path_dir_list = [ "/usr/share/man/man8",]
                  

# platform-dependent paths
myPlatform = platform.dist()[0]

def files_in( d ):
    """
    d is a path to a directory. Return a list of paths to all regular
    files in d
    """
    return [ join(d,f) for f in os.listdir(d) if isfile(join(d,f)) ]

data_files = zip(path_dir_list, map(lambda x: files_in(x[1:]), path_dir_list))
print data_files

setup(name="python-pmdc",
      description="LIGO pmdc",
      version=version,
      author="Jeff Kline",
      author_email="ldr@ligo.org",
      license="GPL v3",
      scripts=glob.glob("usr/sbin/*"),
      data_files=data_files)
