.PHONY: up down logs smoke clean

up:
	docker-compose up -d --build

down:
	docker-compose down

logs:
	docker-compose logs -f

smoke:
	python3 tests/smoke_test.py

clean:
	docker-compose down -v
	find . -type d -name __pycache__ -exec rm -rf {} +
