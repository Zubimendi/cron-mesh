.PHONY: up down migrate run test test-integration lint

up:
	docker-compose up -d
	@echo "waiting for postgres..."
	@until docker-compose exec -T postgres pg_isready -U cronmesh > /dev/null 2>&1; do sleep 1; done
	$(MAKE) migrate

down:
	docker-compose down

migrate:
	docker-compose exec -T postgres psql -U cronmesh -d cronmesh < internal/db/migrations/0001_init.sql

run:
	DATABASE_URL="postgres://cronmesh:cronmesh@localhost:5432/cronmesh?sslmode=disable" \
	HTTP_PORT=8080 \
	QUEUELINE_BASE_URL="http://localhost:8081" \
	TICK_INTERVAL_SECONDS=10 \
	LEADER_RETRY_INTERVAL_SECONDS=2 \
	go run ./cmd/cronmesh

test:
	go test ./...

test-integration:
	DATABASE_URL="postgres://cronmesh:cronmesh@localhost:5432/cronmesh?sslmode=disable" \
	go test -tags=integration ./test/integration/...

lint:
	gofmt -l .
	go vet ./...
