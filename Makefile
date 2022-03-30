.PHONY: airflow spark hive scale-spark minio superset down

pull:
	docker-compose pull

up:
	docker-compose up -d

logs:
	docker-compose logs -f

ps:
	docker-compose ps

down:
	docker-compose down -v

jars_dl:
	sh jarfile_download.sh

clear-images:
	docker image prune --filter="dangling=true"


minio:
	docker-compose up -d minio

airflow:
	docker-compose up -d airflow

spark:
	docker-compose up -d spark-master
	sleep 2
	docker-compose up -d spark-worker

scale-spark:
	docker-compose scale spark-worker=3

hive:
	docker-compose up -d mariadb
	sleep 2
	docker-compose up -d hive

presto-cluster:
	docker-compose up -d presto presto-worker

superset:
	docker-compose up -d superset

presto-cli:
	docker-compose exec presto \
	presto --server localhost:8888 --catalog hive --schema default

to-minio:
	sudo cp -r .storage/data/* .storage/minio/datalake/

exec-mariadb:
	docker-compose exec mariadb mysql -u root -p -h localhost

run-spark:
	docker-compose exec airflow \
	spark-submit --master spark://spark-master:7077 \
	--deploy-mode client --driver-memory 2g --num-executors 2 \
	--packages io.delta:delta-core_2.12:1.0.0 --py-files dags/utils/common.py \
	--jars dags/jars/aws-java-sdk-1.11.534.jar,dags/jars/aws-java-sdk-bundle-1.11.874.jar,dags/jars/delta-core_2.12-1.0.0.jar,dags/jars/hadoop-aws-3.2.0.jar,dags/jars/mariadb-java-client-2.7.4.jar \
	dags/etl/spark_initial.py