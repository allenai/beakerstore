from setuptools import setup

# TODO: license

setup(
    name='beakerstore',
    version='0.1.0',
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
