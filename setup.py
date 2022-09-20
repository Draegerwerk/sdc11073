"""A setuptools based setup module.

See:
https://packaging.python.org/en/latest/distributing.html
https://github.com/pypa/sampleproject
"""

# Always prefer setuptools over distutils
from setuptools import setup, find_packages
# To use a consistent encoding
from codecs import open
import os
import subprocess

version = '1.1.15'

# create a version.py file that is
# a) used for __version__ info
# b) contains current git hash of repo
try:
    gitrev = subprocess.check_output(['git', 'rev-parse', 'HEAD']).strip()
    gitrev = gitrev.decode('ascii')
except:
    import traceback
    print (traceback.format_exc())
    gitrev = '-'

here = os.path.abspath(os.path.dirname(__file__))

# write version file
with open(os.path.join(here, 'sdc11073/version.py'), 'w') as v:
    v.write("#generated file!\nversion='{}'\n\ngitrev='''{}'''".format(version, gitrev))
    
# Get the long description from the README file
with open(os.path.join(here, 'README.rst'), encoding='utf-8') as f:
    long_description = f.read()

dependencies = ['lxml>=2.3',
                "netifaces ; platform_system!='Windows'"]

setup(
    name='sdc11073',
    version=version,
    description='pure python implementation of IEEE11073 SDC protocol',
    long_description=long_description,
    url='https://github.com/Draegerwerk/sdc11073',
    author='Bernd Deichmann',
    author_email='bernd.deichmann@draeger.com',

    # See https://pypi.python.org/pypi?%3Aaction=list_classifiers
    classifiers=[
        # How mature is this project? Common values are
        #   3 - Alpha
        #   4 - Beta
        #   5 - Production/Stable
        'Development Status :: 5 - Production/Stable',

        # Indicate who your project is intended for
        'Intended Audience :: Developers',
        'Topic :: Software Development',

        # Pick your license as you wish (should match "license" above)
        'License :: OSI Approved :: MIT License',

        # Specify the Python versions you support here. In particular, ensure
        # that you indicate whether you support Python 2, Python 3 or both.
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
    ],

    # What does your project relate to?
    keywords='SDC IEEE11073',

    # You can just specify the packages manually here if your project is
    # simple. Or you can use find_packages().
    packages=find_packages(include=['sdc11073', 'sdc11073.*']),

    # Alternatively, if you want to distribute just a my_module.py, uncomment
    # this:
    #   py_modules=["my_module"],

    # List run-time dependencies here.  These will be installed by pip when
    # your project is installed. For an analysis of "install_requires" vs pip's
    # requirements files see:
    # https://packaging.python.org/en/latest/requirements.html
    install_requires=dependencies,
    # List additional groups of dependencies here (e.g. development
    # dependencies). You can install these using the following syntax,
    # for example:
    # $ pip install -e .[dev,test]
#    extras_require={
#        'dev': ['check-manifest'],
#        'test': ['coverage'],
#    },

    # If there are data files included in your packages that need to be
    # installed, specify them here.  If using Python 2.6 or less, then these
    # have to be included in MANIFEST.in as well.
    package_data={
        'sdc11073': ['tutorial/readme.rst',
                     'tutorial/consumer/*.py',
                     'tutorial/provider/*.xml',
                     'tutorial/provider/*.py',
                     'xsd/*.xsd',
                     'ca/*.*',
                     'codings/*.csv'],
    },

    # Although 'package_data' is the preferred approach, in some case you may
    # need to place data files outside of your packages. See:
    # http://docs.python.org/3.4/distutils/setupscript.html#installing-additional-files # noqa
    # In this case, 'data_file' will be installed into '<sys.prefix>/my_data'
#    data_files=[('my_data', ['data/data_file'])],

    # To provide executable scripts, use entry points in preference to the
    # "scripts" keyword. Entry points provide cross-platform support and allow
    # pip to create the appropriate form of executable for the target platform.
#    entry_points={
#        'console_scripts': [
#            'sample=sample:main',
#        ],
#    },
)
