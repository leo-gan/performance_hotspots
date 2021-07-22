Code in the fv/ folder keeps an independent test for the full application.
This code can be executed in the pipeline.

# Self Diagnostics 
The application starts with the Self diagnostics in the fv mode. After the Self diagnostic, 
it finishes and does not run indefinitely 
as it is in the standard mode. 
It finishes with the 1 (True) exit code if the Self diagnostics succeeded or with 0 (False) if 
the Self diagnostics failed.

In the fv mode, we have to set up the AD_TEST_LOCAL environment variable to True.

If we want to test the Elasticsearch connection we set up the AD_TEST_USE_ELASTICSEARCH environment variable 
to True, otherwise to False.
If we don't have the Elasticsearch connection, and the AD_TEST_USE_ELASTICSEARCH is True,  
the application should fail in the second cycle of the Self diagnostics,
If we don't have the Elasticsearch connection, and the AD_TEST_USE_ELASTICSEARCH is False,  
the application should succeed in the second cycle of the Self diagnostics,


## Run from the source code without access to the Elasticsearch
Configuration:
> AD_TEST_LOCAL = True
> 
> AD_TEST_USE_ELASTICSEARCH = False

## Run from the source code with access to the Elasticsearch
Configuration:
> AD_TEST_LOCAL = True
> 
> AD_TEST_USE_ELASTICSEARCH = True

## A command to run:

> 
>python -m tests.fv
> 

# Configuration
The test is managed by the environment variables:

## Test related environment variables

- `AD_TEST_LOCAL` - True/False Default: True. Run test from the source code / from the k8s pod.
- `AD_TEST_USE_ELASTICSEARCH` - True/False Default: True. Run test with/without access to the Elasticsearch.

## Required environment variables
- `KUBECONFIG` - a path to the `kubeconfig` file.
- `ES_CA_CERT` - a path to the Elasticsearch certificate file. Usually it is a `.pam` file.
- `ELASTIC_USER` - a login name for the Elasticsearch account.
- `ELASTIC_PASSWORD` - a password for the Elasticsearch account.

## Optional environment variables
They all have default values.

- `AD_TEST_DELETE_MANIFEST` - default: `1`. If it is `0`, the deployed pod is not deleted after the test end.
- `ELASTIC_HOST` - default: `tigera-secure-es-http.tigera-elasticsearch.svc`
- `ELASTIC_PORT` - default: `9200`. 
- `ANOMALY_DETECTION_JOBS_IMAGE` - default: `gcr.io/unique-caldron-775/cnx/tigera/performance_hotspots:master`.


## Optional settings to the Kubernetes cluster
If you run this fv test locally, you need access to the k8s cluster.
Make sure you install the `kubefwd` utility.

Use these commands:

> export KUBECONFIG=path_to_the_kubeconfig file
>
> sudo -E kubefwd svc -n tigera-elasticsearch


##  Settings for the container tests
- install `docker`. Do [these steps](https://askubuntu.com/questions/477551/how-can-i-use-docker-without-sudo#:~:text=If%20you%20don't%20want,writable%20by%20the%20docker%20group.&text=Either%20do%20a%20newgrp%20docker,activate%20the%20changes%20to%20groups.) to allow docker runs as non-root user.
- install `gcloud`. See the [installation instructions](https://cloud.google.com/sdk/docs/install#deb). 
  Now the test uses only the **Google Container Registry**, `gcr.io`

## Running FV Tests as a Docker Container
`Dockerfile.fv_test` can build the fv as a container with the above environment variables as Docker arguments. 
The image can be built as such

```
	docker build -t "<ad-jobs-fv-image-name>" \
		--build-arg ELASTIC_USER=$(ELASTIC_USER) \
		--build-arg ELASTIC_PASSWORD=$(ELASTIC_PASSWORD) \
		--build-arg ELASTIC_HOST=$(ELASTIC_HOST) \
      ...
		-f /path/to/Dockerfile.fv_test .
```

Running the image:
```
docker run "<ad-jobs-fv-image-name>"
```
---
**NOTE**

The FV tests requires an instance of ElasticSearch running alongside to upload the training data to.  
This instance should then be referenced by all the environment variables prefixed by ELASTIC_*.

---

Alternatively there is a Makefile target that runs the containerized FVs and an Elastic Search instance alongside for the FVs to use.  This target does not test against a running deployment of the anomaly detection jobs project on a kubernetes cluster. 

> `make fv`


# Workflow

## Workflow steps

1. Downloads the AD job manifest from the tigera site.

1. [optional] Configures the jobs by editing the manifest file. If the application image is corrupted, 
   this step fixes it. 
   It also can remove the NetworkPolicy sections from the manifest file.

1. Deploys an AD job pod.
   
   It applies the manifest file to the k8s cluster.
   The application started automatically with the pod deployment. When it starts, it runs the **Self Diagnostics**, 
   which essentially is
   the test compounded from two parts: 
    1. a training + diagnostics cycle on the test datasets 
   (these datasets stored as the `data/*.test_dataset.csv` files). 
   2. a training + diagnostics cycle on the Elasticsearch
   data. This step could finish with failure because of the connection problems, but we still get the result from the first part.

1. Reads the Self Diagnostics report in the pod log.

   The Self Diagnostics outputs the result report. This report sent as the alert in the Elasticsearch. 
   This report is also written into the pod log, where this fv test reads it.

1. Undeploys the AD job pod. This step is conditional. It is executed only if 
   the `AD_TEST_DELETE_MANIFEST` environment variable is `False` or `0`.

1. Asserts the Self Diagnostics report in the pod log result. 
   It is the "result" field of the Self Diagnostics report. It can be "Success" or "Failure", 



