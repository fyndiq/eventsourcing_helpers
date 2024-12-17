from setuptools import find_packages, setup

setup(
    name="eventsourcing-helpers",
    version="1.1.0.rc0",
    description="Helpers for practicing the Event sourcing pattern",
    url="https://github.com/fyndiq/eventsourcing_helpers",
    author="Fyndiq AB",
    author_email="support@fyndiq.com",
    license="MIT",
    packages=find_packages(),
    entry_points={
        "pytest11": ["eventsourcing_helpers = eventsourcing_helpers.messagebus.backends.mock.test"]
    },
    install_requires=[
        "structlog>=17.2.0",
        "jsonpickle>=0.9.6",
        "confluent-kafka-helpers>=1.0.0",
    ],
    extras_require={
        "mongo": ["pymongo>=3.6.1"],
        "redis": ["redis>=2.10.6", "hiredis>=0.2.0"],
        "cnamedtuple": ["cnamedtuple>=0.1.6"],
    },
    zip_safe=False,
)
