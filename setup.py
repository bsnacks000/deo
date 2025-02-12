import io
import os
import sys
from shutil import rmtree

from setuptools import find_packages, setup, Command

# Package meta-data.
NAME = 'deo'
DESCRIPTION = 'another jsonrpc2-http implementation'
URL = 'https://github.com/bsnacks000/deo'
EMAIL = 'bsnacks000@gmail.com'
AUTHOR = 'bsnacks000'
REQUIRES_PYTHON = '>=3.6.0'
VERSION = '0.1.0'

# What packages are required for this module to be executed?
REQUIRED_PACKAGES = [
    'marshmallow>=3.1',
    'aiohttp>=3', 
    'coloredlogs', 
    'requests', 
    'rapidjson'
]

TESTS_REQUIRE = [
    'pytest', 
    'pytest-asyncio', 
    'asynctest', 
    'pytest-cov'
]

SETUP_REQUIRES = [
    'pytest-runner'
] 

# What packages are optional?
EXTRAS = {
    'dask': ['dask', 'distributed'],
}

here = os.path.abspath(os.path.dirname(__file__))

# Import the README and use it as the long-description.
# Note: this will only work if 'README.md' is present in your MANIFEST.in file!
try:
    with io.open(os.path.join(here, 'README.md'), encoding='utf-8') as f:
        long_description = '\n' + f.read()
except FileNotFoundError:
    long_description = DESCRIPTION

# Load the package's __version__.py module as a dictionary.
about = {}
if not VERSION:
    project_slug = NAME.lower().replace("-", "_").replace(" ", "_")
    with open(os.path.join(here, project_slug, '__version__.py')) as f:
        exec(f.read(), about)
else:
    about['__version__'] = VERSION


# Where the magic happens:
setup(
    name=NAME,
    version=about['__version__'],
    description=DESCRIPTION,
    long_description=long_description,
    long_description_content_type='text/markdown',
    author=AUTHOR,
    author_email=EMAIL,
    python_requires=REQUIRES_PYTHON,
    url=URL,
    packages=find_packages(exclude=["tests", "*.tests", "*.tests.*", "tests.*","examples"]),
    install_requires=REQUIRED_PACKAGES,
    extras_require=EXTRAS,
    include_package_data=True,
    license='MIT',
    setup_requires=SETUP_REQUIRES, 
    tests_require=TESTS_REQUIRE,
    classifiers=[
        # Trove classifiers
        # Full list: https://pypi.python.org/pypi?%3Aaction=list_classifiers
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: Implementation :: CPython',
        'Programming Language :: Python :: Implementation :: PyPy'
    ],
)