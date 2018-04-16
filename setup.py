from setuptools import find_packages, setup

setup(
    name="eventsourcing-helpers",
    version="0.7.0",
    description="Helpers for practicing the Event sourcing pattern",
    url="https://github.com/fyndiq/eventsourcing_helpers",
    author="Fyndiq AB",
    author_email="support@fyndiq.com",
    license="MIT",
    packages=find_packages(),
    install_requires=[
        'structlog>=17.2.0',
        'cnamedtuple>=0.1.6',
        'confluent-kafka-helpers==0.5.2',
        'jsonpickle==0.9.6',
    ],
    extras_requires={
        'mongo': ["pymongo==3.6.1"],
        'redis': ["redis>=2.10.6", "hiredis>=0.2.0"],
    },
    dependency_links=[
        'git+https://github.com/fyndiq/confluent_kafka_helpers@v0.5.2#egg=confluent-kafka-helpers-0.5.2'  # noqa
    ],
    zip_safe=False
)
