from setuptools import setup

setup(name="eventsourcing-helpers",
      version="0.1",
      description="Helpers for practicing the Event sourcing pattern",
      url="https://github.com/fyndiq/eventsourcing_helpers",
      author="Fyndiq AB",
      author_email="support@fyndiq.com",
      license="MIT",
      packages=["eventsourcing_helpers"],
      install_requires=[
          'structlog==17.2.0'
      ],
      zip_safe=False)
