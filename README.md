# anomaly_detection_elastic_jobs

Custom anomaly detection based on the existed ElasticSearch ML Jobs

## Local Build

It is recommended to run python packages in a virtual environment to avoid contention of different versions of package dependencies for multiple projects.

> `make install-venv`

will create a virtual environment named `venv` with the repository's dependencies listed in `requirements.txt`.

The environment can then be acccessed with 

> `source venv/bin/activate`

and all `python` commands will then refer to the packages installed in venv


## Unit Tests

To run all UTs:

> `make ut`

The UT target creates a virtual env with the required dependencies from requirements.txt installed and runs the tests defined in `tests/ut` within the tests' virtual environment.

## FV Tests

To run FVs:

> `make fv`

Further details can be found in the [FV's README](tests/fv/README.md)  