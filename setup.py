from setuptools import find_packages, setup

setup(
    name="eventsourcing-helpers",
    version="0.8.1",
    description="Helpers for practicing the Event sourcing pattern",
    url="https://github.com/fyndiq/eventsourcing_helpers",
    author="Fyndiq AB",
    author_email="support@fyndiq.com",
    license="MIT",
    packages=find_packages(),
    install_requires=[
        'structlog>=17.2.0',
        'cnamedtuple>=0.1.6',
        'jsonpickle==0.9.6',
        'confluent-kafka-helpers==0.6.1'
    ],
    extras_require={
        'mongo': ["pymongo==3.6.1"],
        'redis': ["redis>=2.10.6", "hiredis>=0.2.0"],
    },
    zip_safe=False
)
