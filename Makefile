.PHONY: help up down nuke ps logs logs-grafana logs-timescale logs-redpanda psql rpk

COMPOSE := docker compose -f docker/docker-compose.yml --env-file .env

help:  ## Show available targets
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'

up:  ## Start all services in the background
	$(COMPOSE) up -d

down:  ## Stop all services, keep volumes
	$(COMPOSE) down

nuke:  ## Stop all services AND wipe volumes (fresh start)
	$(COMPOSE) down -v

ps:  ## Show status of all services
	$(COMPOSE) ps

logs:  ## Tail logs from all services
	$(COMPOSE) logs -f --tail=50

logs-grafana:  ## Tail logs from grafana only
	$(COMPOSE) logs -f --tail=50 grafana

logs-timescale:  ## Tail logs from timescaledb only
	$(COMPOSE) logs -f --tail=50 timescaledb

logs-redpanda:  ## Tail logs from redpanda only
	$(COMPOSE) logs -f --tail=50 redpanda-0

psql:  ## Open psql shell in timescaledb
	docker exec -it timescaledb psql -U postgres -d market_data

rpk:  ## Open rpk shell in redpanda
	docker exec -it redpanda-0 rpk cluster info

db-reset:  ## Wipe and re-initialize the database
	$(COMPOSE) stop timescaledb
	$(COMPOSE) rm -f timescaledb
	docker volume rm oil-kzt-monitor_timescale_data || true
	$(COMPOSE) up -d timescaledb
	@echo "Waiting for timescaledb to be healthy..."
	@sleep 15
	@echo "TimescaleDB re-initialized with init-sql/ scripts."