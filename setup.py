from setuptools import setup

setup(
    name='lsh',
    version='0.0.1',
    description='pure python near-duplicate document detection system',
    url='http://www.github.com/embr/lsh',
    author='Evan Rosen',
    author_email='rosen21@gmail.com',
    entry_points = {
        'console_scripts': [
            'lsh = lsh.lsh_app:main',
            ]
        },
    install_requires=[
       "python-Levenshtein >= 0.10.2",
       ]
    )
