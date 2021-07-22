ELASTICSEARCH_CONTAINER_NAME=$1
BOOTSTRAP_PASSWORD=$2
ELASTICSEARCH_RUN_SECURITY_ARGS="-e xpack.security.enabled=true -e ELASTIC_PASSWORD=${BOOTSTRAP_PASSWORD}"
EXTRA_CURL_ARGS="-u elastic:${BOOTSTRAP_PASSWORD}"
ELASTICSEARCH_EXEC_SECURITY_ARGS="-e BOOTSTRAP_PASSWORD=${BOOTSTRAP_PASSWORD} -e ELASTIC_PASSWORD=${BOOTSTRAP_PASSWORD}"
FV_ELASTICSEARCH_IMAGE="docker.elastic.co/elasticsearch/elasticsearch:7.3.0"

run_elasticsearch()
{
	echo "Starting elasticsearch"
	docker run \
		--name ${ELASTICSEARCH_CONTAINER_NAME} \
		--detach \
		-p 9200:9200 \
		-p 9300:9300 \
		-e "discovery.type=single-node" \
		${ELASTICSEARCH_RUN_SECURITY_ARGS} \
		-v ${PACKAGE_ROOT}/test:/test:ro \
		${FV_ELASTICSEARCH_IMAGE}

	until docker exec ${ELASTICSEARCH_CONTAINER_NAME} curl http://127.0.0.1:9200 ${EXTRA_CURL_ARGS} 2> /dev/null;
	do
		echo "Waiting for Elasticsearch to start..."; \
		sleep 5
	done

	echo "Elasticsearch is running."
}

docker stop ${ELASTICSEARCH_CONTAINER_NAME} || true
docker rm -f ${ELASTICSEARCH_CONTAINER_NAME} || true
	
run_elasticsearch
