.PHONY: airflow spark-master spark-worker scale-spark minio down

minio:
	docker-compose up -d minio

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