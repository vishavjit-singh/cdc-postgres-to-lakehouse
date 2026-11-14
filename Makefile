PY ?= python3
export PYTHONPATH := src

.PHONY: install demo simulate consume reconcile report summary test lint stack stack-down clean

install:
	$(PY) -m pip install -r requirements-dev.txt

## Local end to end on the SYNTHETIC Debezium stream: 6 batches → crash mid-batch → recover → finish → reconcile
demo: clean simulate
	$(PY) -m cdc_lake consume --max-batches 6
	@echo "--- simulated crash: bronze written, checkpoint not committed ---"
	-$(PY) -m cdc_lake consume --crash-after-bronze 9
	@echo "--- restart: uncommitted bronze removed, batch replayed from the checkpoint ---"
	$(PY) -m cdc_lake consume
	$(PY) -m cdc_lake reconcile
	$(PY) -m cdc_lake report
	$(PY) -m cdc_lake summary

simulate:
	$(PY) -m cdc_lake simulate

consume:
	$(PY) -m cdc_lake consume

reconcile:
	$(PY) -m cdc_lake reconcile

report:
	$(PY) -m cdc_lake report

summary:
	$(PY) -m cdc_lake summary

test:
	$(PY) -m pytest -q

lint:
	$(PY) -m ruff check src tests

## Real PostgreSQL + Kafka + Debezium (needs Docker)
stack:
	docker compose up -d --build postgres kafka connect
	docker compose run --rm register
	docker compose run --rm workload
	docker compose run --rm consumer
	docker compose run --rm reconcile

stack-down:
	docker compose down -v

clean:
	rm -rf data/stream data/lake
