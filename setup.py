from setuptools import setup

version = {}
with open('beakerstore/version.py') as v:
    exec(v.read(), version)

# TODO: license

setup(
    name='beakerstore',
    version=version['__version__'],
    description='Local store for Beaker datasets and files.',
    packages=['beakerstore'],
    url='https://github.com/allenai/beakerstore',
    author='Chloe Anastasiades',
    author_email='chloea@allenai.org',
    python_requires='>=3',
    install_requires=[
        'requests >= 2.22.0'
    ]
)
