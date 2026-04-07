.PHONY: help build test up down deploy minikube seed simulate chaos audit check-deps

COMPOSE := ./scripts/compose.sh

API_KEY ?= local-upload-key
INGESTION_URL ?= http://localhost:8080
ROUTING_URL ?= http://localhost:8081

help:
	@echo "LCR Platform"
	@echo "  make check-deps - Verify docker, compose, and daemon"
	@echo "  make build      - Build all Docker images"
	@echo "  make up         - Start local stack (docker compose)"
	@echo "  make down       - Stop local stack"
	@echo "  make seed       - Upload sample rate sheets"
	@echo "  make route      - Test routing for a UK number"
	@echo "  make simulate   - Run traffic simulator"
	@echo "  make audit      - Run invoice auditor"
	@echo "  make dashboard  - Start React dashboard (dev)"
	@echo "  make minikube   - Start Minikube and deploy"
	@echo "  make deploy     - Deploy to Minikube"

check-deps:
	@./scripts/compose.sh version
	@docker info >/dev/null 2>&1 || (echo "ERROR: Docker daemon not running. Start Colima or Docker Desktop." && exit 1)
	@echo "All Docker dependencies OK."

build:
	$(COMPOSE) build

up:
	$(COMPOSE) up -d
	./scripts/wait-for-services.sh
	$(MAKE) seed

down:
	$(COMPOSE) down -v

seed:
	./scripts/seed/upload-rates.sh

route:
	curl -s -X POST $(ROUTING_URL)/route \
		-H 'Content-Type: application/json' \
		-d '{"dialedNumber":"447700900123","defaultRegion":"GB"}' | python3 -m json.tool

simulate:
	$(COMPOSE) --profile simulate run --rm traffic-simulator

audit:
	$(COMPOSE) --profile audit run --rm invoice-auditor

dashboard:
	cd dashboard && npm install && npm run dev

dashboard-up:
	$(COMPOSE) up -d --build dashboard
	@echo "Dashboard: http://localhost:3000"

test-go:
	cd services/ingestion && go test ./...
	cd services/telemetry && go test ./...

test-java:
	cd services/routing-engine && mvn test -q

test: test-go test-java

minikube:
	./infra/minikube/start.sh

deploy:
	eval $$(minikube docker-env) && $(MAKE) build
	kubectl apply -k infra/k8s/base
	kubectl apply -k infra/k8s/ingestion
	kubectl apply -k infra/k8s/routing-engine
	kubectl apply -k infra/k8s/telemetry
	kubectl apply -k infra/k8s/mock-carrier
	kubectl apply -k infra/k8s/invoice-auditor

chaos-carrier:
	./scripts/chaos/inject-carrier-failure.sh

chaos-verify:
	./scripts/chaos/verify-failover.sh
