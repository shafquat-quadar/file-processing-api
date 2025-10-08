PYTHON=python3
PIP=pip3
APP=risk_query_service.app:app
IMAGE=risk-query-service

.PHONY: install run test docker fmt

install:
	$(PIP) install -r requirements.txt

run:
	uvicorn $(APP) --reload

test:
	pytest

docker:
	docker build -t $(IMAGE) .

fmt:
	@echo "No formatters configured"
