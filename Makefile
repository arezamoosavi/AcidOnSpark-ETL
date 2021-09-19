.PHONY: airflow spark-master spark-worker scale-spark down

airflow:
	docker-compose up -d airflow

spark-master:
	docker-compose up -d spark-master

spark-worker:
	docker-compose up -d spark-worker

scale-spark:
	docker-compose scale spark-worker=3

down:
	docker-compose down -v