Code in the tests/ut/ folder keeps the unit tests for the full application.

# Configuration
Run tests with the pytest utility.

## PyCharm configuration
In the PyCharm use Run / Edit configurations... / Python Tests / '+' button


## command line configuration
In the terminal:
* The working directory should be `ph/`
* set up the environment variable `PYTHONPATH=<project_path>/ph`


## Required environment variables for the test_model_processor.py tests

- `ES_CA_CERT` - a path to the Elasticsearch certificate file. Usually it is a `.pam` file.
- `ELASTIC_USER` - a login name for the Elasticsearch account.
- `ELASTIC_PASSWORD` - a password for the Elasticsearch account.
 


