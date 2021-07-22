PACKAGE_NAME?=github.com/tigera/performance_hotspots
GO_BUILD_VER?=v0.53

ADJ_IMAGE             ?=tigera/performance_hotspots
BUILD_IMAGES          ?=$(ADJ_IMAGE)
DEV_REGISTRIES        ?=gcr.io/unique-caldron-775/cnx
RELEASE_REGISTRIES    ?=quay.io
RELEASE_BRANCH_PREFIX ?=release-calient
DEV_TAG_SUFFIX        ?=calient-0.dev
ARCHES                ?=amd64
###############################################################################
# Download and include Makefile.common
#   Additions to EXTRA_DOCKER_ARGS need to happen before the include since
#   that variable is evaluated when we declare DOCKER_RUN and siblings.
###############################################################################
MAKE_BRANCH?=$(GO_BUILD_VER)
MAKE_REPO?=https://raw.githubusercontent.com/projectcalico/go-build/$(MAKE_BRANCH)

Makefile.common: Makefile.common.$(MAKE_BRANCH)
	cp "$<" "$@"
Makefile.common.$(MAKE_BRANCH):
	# Clean up any files downloaded from other branches so they don't accumulate.
	rm -f Makefile.common.*
	curl --fail $(MAKE_REPO)/Makefile.common -o "$@"

include Makefile.common

AD_JOB_FV_IMAGE ?= tigera/performance_hotspots-fv
ADJ_FV_TEST_DOCKERFILE=./tests/adj_test_dockerfiles/Dockerfile.fv_test

ES_CONTAINER_NAME="ad-job-fv-elasticsearch"
ELASTIC_USER=elastic
BOOTSTRAP_PASSWORD:=$(shell cat /dev/urandom | LC_CTYPE=C tr -dc A-Za-z0-9 | head -c16)
ELASTIC_PASSWORD=$(BOOTSTRAP_PASSWORD)
# retrieves net ip of host as fv docker contianer needs to communicate with the elastichsearch container
ELASTIC_HOST:=$(shell hostname -I | cut -f1 -d' ')

image:
	docker build -t $(ADJ_IMAGE):latest-$(ARCH) -f Dockerfile .
ifeq ($(ARCH),amd64)
	docker tag $(ADJ_IMAGE):latest-$(ARCH) $(ADJ_IMAGE):latest
endif

.PHONY: ci cd
ci: ut fv

cd: image cd-common

##########################################################################################
# LOCAL BUILD
##########################################################################################

.PHONY: install-venv
install-venv:
	virtualenv venv
	( \
		. ./venv/bin/activate; \
		pip3 install -r requirements.txt; \
	)

##########################################################################################
# TESTS
##########################################################################################

.PHONY: test
test: image

# UT
PYTEST_SETUP_ENV_VARS=PYTHONDONTWRITEBYTECODE=1  PYTHONPATH=./

.PHONY: ut
ut:
ifeq ("$(SEMAPHORE)", "true")
	pip3 install -r requirements.txt
	$(PYTEST_SETUP_ENV_VARS) python3 -m pytest tests/ut
else
	$(MAKE) ut-local
endif

ut-local:
	virtualenv venv-test
	( \
		. ./venv-test/bin/activate; \
		pip3 install -r requirements.txt; \
		$(PYTEST_SETUP_ENV_VARS) python3 -m pytest tests/ut/ ;\
		deactivate; \
	)
	rm -rf venv-test

# FV
.PHONY: fv
fv: run_elasticsearch fv-test-image
	docker run $(AD_JOB_FV_IMAGE):latest
	docker rm -f $(ES_CONTAINER_NAME)

fv-test-image:
	docker build -t $(AD_JOB_FV_IMAGE):latest \
		--build-arg ELASTIC_USER=$(ELASTIC_USER) \
		--build-arg ELASTIC_PASSWORD=$(ELASTIC_PASSWORD) \
		--build-arg ELASTIC_HOST=$(ELASTIC_HOST) \
		-f $(ADJ_FV_TEST_DOCKERFILE) .

.PHONY: run_elasticsearch
run_elasticsearch:
	@echo ${ELASTIC_PASSWORD}
	chmod +x ./tests/fv/run_elasticsearch.sh
	./tests/fv/run_elasticsearch.sh ${ES_CONTAINER_NAME} $(BOOTSTRAP_PASSWORD)
