from setuptools import setup


setup(
    name='beakerstore',
    version='0.1.0',
    description='Local store for Beaker datasets and files.',
    py_modules=['beakerstore.beakerstore'],
    test_suite='beakerstore_test.py',
    tests_require=['pytest']
)
