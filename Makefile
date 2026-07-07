# Convenience targets for LakeStream. Requires sbt + docker + kind + kubectl.
.PHONY: help fmt test coverage build assembly image compose-up compose-down demo \
        kind-up kind-down k8s-deploy k8s-delete produce verify optimize clean

help:
	@echo "LakeStream make targets:"
	@echo "  fmt          - apply scalafmt"
	@echo "  test         - run ScalaTest specs"
	@echo "  coverage     - run tests with scoverage + HTML report"
	@echo "  assembly     - build the fat JAR"
	@echo "  image        - build the Docker image (lakestream:local)"
	@echo "  compose-up   - run Kafka + MinIO + job via docker compose"
	@echo "  demo         - compose 'demo' profile: seed 2000 events, then verify"
	@echo "  compose-down - tear the compose stack down"
	@echo "  kind-up      - create the kind cluster + deploy everything"
	@echo "  kind-down    - delete the kind cluster"
	@echo "  produce      - publish 2000 demo events (needs assembly + running Kafka)"
	@echo "  verify       - assert Delta count == distinct(eventId) (PASS/FAIL)"
	@echo "  optimize     - run OPTIMIZE (ZORDER) + VACUUM on the Delta table"

fmt:
	sbt scalafmtAll

test:
	sbt test

coverage:
	sbt clean coverage test coverageReport
	@echo "HTML report: target/scala-2.12/scoverage-report/index.html"

assembly:
	sbt assembly

image:
	docker build -t lakestream:local .

compose-up:
	docker compose up --build

demo:
	docker compose --profile demo up --build

compose-down:
	docker compose --profile demo down -v

kind-up:
	kind create cluster --name lakestream --config k8s/kind-cluster.yaml
	docker build -t lakestream:local .
	kind load docker-image lakestream:local --name lakestream
	kubectl apply -f k8s/00-namespace.yaml
	kubectl apply -f k8s/10-minio.yaml
	kubectl apply -f k8s/20-kafka.yaml
	kubectl apply -f k8s/30-lakestream.yaml

kind-down:
	kind delete cluster --name lakestream

k8s-deploy:
	kubectl apply -f k8s/00-namespace.yaml -f k8s/10-minio.yaml -f k8s/20-kafka.yaml -f k8s/30-lakestream.yaml

k8s-delete:
	kubectl delete -f k8s/30-lakestream.yaml -f k8s/20-kafka.yaml -f k8s/10-minio.yaml -f k8s/00-namespace.yaml || true

produce:
	java -cp target/scala-2.12/lakestream-assembly-0.1.0.jar \
		dev.xj16.lakestream.tools.EventProducer localhost:9092 events 2000 0.1

# Programmatic proof of the core guarantee (needs a running MinIO/S3 + the JAR).
verify:
	docker compose run --rm -e MAIN_CLASS=dev.xj16.lakestream.tools.DeltaVerify lakestream

# One-shot Delta compaction + vacuum.
optimize:
	docker compose run --rm \
		-e MAIN_CLASS=dev.xj16.lakestream.tools.DeltaMaintenance \
		-e MAINTENANCE_ENABLED=true lakestream

clean:
	sbt clean
