import os

from setuptools import find_packages, setup


def get_requirements(path):
    content = open(os.path.join(os.path.dirname(__file__), path)).read()
    return [req for req in content.split("\n") if req != '' and not req.startswith('#')]


setup(
    name="eventsourcing-helpers",
    version="0.8.4",
    description="Helpers for practicing the Event sourcing pattern",
    url="https://github.com/fyndiq/eventsourcing_helpers",
    author="Fyndiq AB",
    author_email="support@fyndiq.com",
    license="MIT",
    packages=find_packages(),
    install_requires=get_requirements('requirements/setup.txt'),
    extras_require={
        'mongo': get_requirements('requirements/mongo.txt'),
        'redis': get_requirements('requirements/redis.txt'),
    },
    zip_safe=False,
)
