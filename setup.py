from setuptools import setup, find_packages

setup(name="eventsourcing-helpers",
      version="0.4.1",
      description="Helpers for practicing the Event sourcing pattern",
      url="https://github.com/fyndiq/eventsourcing_helpers",
      author="Fyndiq AB",
      author_email="support@fyndiq.com",
      license="MIT",
      packages=find_packages(),
      install_requires=[
          'structlog==17.2.0',
          'confluent-kafka-helpers>=0'
      ],
      dependency_links=[
          'git+https://github.com/fyndiq/confluent_kafka_helpers#egg=confluent-kafka-helpers-0'
      ],
      zip_safe=False)
