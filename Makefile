up:
	docker-compose up --build

down:
	docker-compose down -v


kafka:
	docker-compose up -d zookeeper
	docker-compose up -d kafka
	docker-compose up -d kafkacat

pg:
	docker-compose up -d postgres

spark:
	docker-compose up --build -d spark

jupyter:
	docker-compose up -d jupyter
