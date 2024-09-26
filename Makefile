unit-test:
	python -m unittest tests.unit_tests
run-redis:
	docker run -d --name redis-stack-server -p 6379:6379 redis/redis-stack-server:latest
run-redis-stack:
	docker run -d --name redis-stack -p 6379:6379 -p 8001:8001 redis/redis-stack:latest
connect-redis:
	docker exec -it redis-stack-server redis-cli