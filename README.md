# Event sourcing helpers
Python helpers for practicing the event sourcing pattern using DDD.

[![CircleCI](https://circleci.com/gh/fyndiq/eventsourcing_helpers/tree/master.svg?style=shield)](https://circleci.com/gh/fyndiq/eventsourcing_helpers/tree/master)
[![codecov](https://codecov.io/gh/fyndiq/eventsourcing_helpers/branch/master/graph/badge.svg)](https://codecov.io/gh/fyndiq/eventsourcing_helpers)

### Creating new releases
 - Merge your feature PR into master
 - In the master branch bump the version in `setup.py`
 - Verify that everything installs correctly locally (eg. `python setup.py
   install`)
 - Create a new release
    - The title should be the same as the tag version (vX.Y.Z)
    - In the description make a bullet list with the changes. Preferably you
      also add links to all affected PRs
 - Pull the latest master locally
 - Clean up the old distribution `rm dist/*`
 - Create a new distribution `python setup.py sdist bdist_wheel`
 - Upload the new distribution to PyPi `twine upload dist/*`
