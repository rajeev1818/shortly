.PHONY: up down logs ps \
        psql redis-cli kafka-produce kafka-consume kafka-topics proto

up:
	docker compose up -d
	@echo "Waiting for services to be healthy..."
	@docker compose wait postgres redis kafka 2>/dev/null || true
	@echo "All services up."

down:
	docker compose down

logs:
	docker compose logs -f

ps:
	docker compose ps

# ── Connection helpers ────────────────────────────────────────────────────────

psql:
	docker exec -it shortly_postgres psql -U shortly -d shortly

redis-cli:
	docker exec -it shortly_redis redis-cli

# Publish one message; hit Ctrl-C when done.
kafka-produce:
	docker exec -it shortly_kafka \
		/opt/kafka/bin/kafka-console-producer.sh \
		--bootstrap-server localhost:9092 \
		--topic shortly-events

# Consume from the beginning of shortly-events.
kafka-consume:
	docker exec -it shortly_kafka \
		/opt/kafka/bin/kafka-console-consumer.sh \
		--bootstrap-server localhost:9092 \
		--topic shortly-events \
		--from-beginning

kafka-topics:
	docker exec shortly_kafka \
		/opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list

proto:
	protoc \
		--go_out=. --go_opt=paths=source_relative \
		--go-grpc_out=. --go-grpc_opt=paths=source_relative \
		proto/shortener.proto
