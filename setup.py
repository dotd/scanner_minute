from codecs import open
from os import path

from setuptools import find_packages, setup

here = path.abspath(path.dirname(__file__))

# Get the long description from the README file
with open(path.join(here, "README.md"), encoding="utf-8") as f:
    long_description = f.read()

setup(
    name="ScannerMinute",
    version="0.0.1",
    description="ScannerMinute",
    long_description=long_description,
    url="https://github.com/dotd/scanner_minute",
    python_requires=">=3.9",
    install_requires=[],
    packages=find_packages(exclude=["docs", "notebooks", "scripts"]),
)
