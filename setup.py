from setuptools import setup, find_packages

setup(
    name='remote_data',
    version='0.1.0',
    packages=['visionlab.remote_data'] + 
             ['visionlab.remote_data.' + p for p in find_packages(where='remote_data')],
    package_dir={'visionlab.remote_data': 'remote_data'},
    python_requires='>=3.3',
    # Other metadata such as classifiers, description, etc.
)