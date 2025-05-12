from setuptools import setup, find_packages

with open("README.md", 'r') as f:
    long_description = f.read()

def parse_requirements(filename):
    with open(filename, 'r') as f:
        return [line.strip() for line in f if line.strip() and not line.startswith('#')]

setup(
    name='crocolakeloader',
    version='0.0.1',
    description='Package to load CrocoLake',
    license="GNU GPLv3",
    long_description=long_description,
    author='Enrico Milanese',
    author_email='enrico.milanese@whoi.edu',
    packages=find_packages(),
    install_requires=parse_requirements('requirements.txt'),
    entry_points={
        'console_scripts': [
            'crocolaketools = scripts.main:main',
            'download_db = scripts.download_db:main',
        ],
    },
)
