.PHONY: help up down restart logs migrate seed setup-db reset-db redis-geo test-order

# Cores para saída formatada no terminal
GREEN  := $(shell tput -Txterm setaf 2)
YELLOW := $(shell tput -Txterm setaf 3)
WHITE  := $(shell tput -Txterm setaf 7)
RESET  := $(shell tput -Txterm sgr0)

## help: Exibe esta lista de comandos com explicações
help:
	@echo ''
	@echo 'Uso:'
	@echo '  ${YELLOW}make${RESET} ${GREEN}<target>${RESET}'
	@echo ''
	@echo 'Targets disponíveis:'
	@awk '/^[a-zA-Z\-\_0-9]+:/ { \
		helpMessage = match(lastLine, /^## (.*)/); \
		if (helpMessage) { \
			helpCommand = substr($$1, 1, length($$1)-1); \
			helpMessage = substr(lastLine, RSTART + 3, RLENGTH - 3); \
			printf "  ${GREEN}%-15s${RESET} %s\n", helpCommand, helpMessage; \
		} \
	} \
	{ lastLine = $$0 }' $(MAKEFILE_LIST)

## up: Sobe todos os containers da infraestrutura via Docker Compose
up:
	docker compose up -d

## down: Para e remove todos os containers da aplicação
down:
	docker compose down

## restart: Reinicia a aplicação e exibe os logs
restart: down up logs

## logs: Acompanha os logs da aplicação Go em tempo real
logs:
	docker logs -f ifood_service_api

## migrate: Executa o schema init.sql no PostgreSQL
migrate:
	@echo "${YELLOW}Applying database migrations...${RESET}"
	docker exec -i ifood_poc_postgres psql -U postgres -d ifood_db < init.sql

## seed: Compila e executa o script de povoamento (stores e couriers) no Postgres/Redis
seed:
	@echo "${YELLOW}Seeding PostgreSQL and Redis...${RESET}"
	docker run --rm \
		--network=$$(docker network ls --filter name=go_delivery -q | head -n 1) \
		-e DB_HOST=postgres \
		-e REDIS_ADDR=redis:6379 \
		-v $$(pwd):/app \
		-w /app \
		golang:1.26-alpine go run cmd/seed/main.go

## setup-db: Prepara a base do zero (Aplica migrations e executa o seed)
setup-db: migrate seed

## reset-db: Reseta as tabelas e reexecuta o setup do banco de dados
reset-db:
	@echo "${YELLOW}Resetting database and Redis cache...${RESET}"
	docker exec -i ifood_poc_postgres psql -U postgres -d ifood_db -c "DROP SCHEMA public CASCADE; CREATE SCHEMA public;"
	docker exec -i ifood_poc_redis redis-cli FLUSHALL
	$(MAKE) setup-db

## redis-geo: Consulta a posição dos entregadores em relação ao Burger King Pinheiros
redis-geo:
	@echo "${GREEN}Consultando entregadores em um raio de 5km do Burger King Pinheiros:${RESET}"
	docker exec -it ifood_poc_redis redis-cli GEOSEARCH couriers:locations:sp FROMLONLAT -46.655982 -23.561679 BYRADIUS 5 km WITHDIST WITHCOORD

## test-order: Cria um pedido de teste no Burger King Pinheiros via API
test-order:
	@echo "${GREEN}Enviando pedido de teste...${RESET}"
	curl -X POST http://localhost:8080/orders \
		-H "Content-Type: application/json" \
		-d '{"store_id":"11111111-1111-1111-1111-111111111111","customer":{"name":"Luis","phone":"+5511999999999"},"delivery_location":{"address":"Rua Oscar Freire, 500","latitude":-23.562500,"longitude":-46.660000},"items":[{"product_id":"22222222-2222-2222-2222-222222222222","quantity":2,"unit_price":35.00}]}'
	@echo ''

# ==========================================
# SIMULAÇÃO DO FLUXO COMPLETO (E2E)
# ==========================================

## simulate-order: Simula a criação de um pedido pelo cliente
simulate-order:
	@echo "${GREEN}1. [CLIENTE] Criando novo pedido no Burger King...${RESET}"
	@curl -s -X POST http://localhost:8080/orders \
		-H "Content-Type: application/json" \
		-d '{"store_id":"11111111-1111-1111-1111-111111111111","customer":{"name":"Luis","phone":"+5511999999999"},"delivery_location":{"address":"Rua Oscar Freire, 500","latitude":-23.562500,"longitude":-46.660000},"items":[{"product_id":"22222222-2222-2222-2222-222222222222","quantity":2,"unit_price":35.00}]}' | tee /tmp/last_order.json
	@echo ''

## simulate-kds: Avanca o status do último pedido na cozinha (PREPARING -> READY)
simulate-kds:
	@ORDER_ID=$$(jq -r '.id' /tmp/last_order.json 2>/dev/null || echo ""); \
	if [ -z "$$ORDER_ID" ] || [ "$$ORDER_ID" = "null" ]; then \
		echo "${YELLOW}Nenhum pedido recente em /tmp/last_order.json. Execute 'make simulate-order' primeiro.${RESET}"; \
		exit 1; \
	fi; \
	echo "${GREEN}2. [COZINHA] Iniciando preparo do pedido $$ORDER_ID...${RESET}"; \
	curl -s -X PATCH http://localhost:8080/orders/$$ORDER_ID/status -H "Content-Type: application/json" -d '{"status":"IN_PREPARATION"}' || true; \
	echo ''; \
	sleep 2; \
	echo "${GREEN}3. [COZINHA] Pedido pronto para entrega!${RESET}"; \
	curl -s -X PATCH http://localhost:8080/orders/$$ORDER_ID/status -H "Content-Type: application/json" -d '{"status":"READY_FOR_DELIVERY"}' || true; \
	echo ''

## simulate-dispatch: Solicita o despacho e valida no banco e Redis
simulate-dispatch:
	@ORDER_ID=$$(jq -r '.id' /tmp/last_order.json 2>/dev/null || echo ""); \
	if [ -z "$$ORDER_ID" ] || [ "$$ORDER_ID" = "null" ]; then \
		echo "${YELLOW}Nenhum pedido recente. Execute 'make simulate-order' primeiro.${RESET}"; \
		exit 1; \
	fi; \
	echo "${GREEN}4. [DISPATCH] Garantindo status IDLE para o Carlos Motoboy no Redis...${RESET}"; \
	docker exec -i ifood_poc_redis redis-cli SET "courier:aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:status" "IDLE" > /dev/null; \
	echo "${GREEN}5. [DISPATCH] Solicitando despacho do pedido $$ORDER_ID...${RESET}"; \
	curl -s -X POST http://localhost:8080/orders/$$ORDER_ID/dispatch \
		-H "Content-Type: application/json" \
		-d '{"status":"OUT_FOR_DELIVERY","courier_id":"aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"}' || true; \
	echo ''; \
	sleep 1; \
	echo "${GREEN}6. [DISPATCH] Verificando status atualizado do pedido no Postgres:${RESET}"; \
	docker exec -it ifood_poc_postgres psql -U postgres -d ifood_db -c "SELECT id, store_id, status FROM orders WHERE id = '$$ORDER_ID';"

## simulate-e2e: Executa todo o ciclo do zero (Reset -> Seed -> Pedido -> Cozinha -> Despacho)
simulate-e2e: reset-db simulate-order simulate-kds simulate-dispatch
	@echo ""
	@echo "${GREEN}🎉 Simulação Ponta a Ponta concluída com sucesso!${RESET}"