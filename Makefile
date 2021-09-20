.PHONY: airflow spark scale-spark minio down

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

down:
	docker-compose down -v


run-spark:
	spark-submit --master spark://spark-master:7077 \
	--deploy-mode client --driver-memory 2g \
	--num-executors 2 \
	--jars dags/jars/aws-java-sdk-1.11.534.jar,\
	dags/jars/aws-java-sdk-bundle-1.11.874.jar,\
	dags/jars/delta-core_2.12-1.0.0.jar,\
	dags/jars/hadoop-aws-3.2.0.jar,\
	dags/jars/aws-java-sdk-core-1.11.534.jar,\
	dags/jars/aws-java-sdk-dynamodb-1.11.534.jar,\
	dags/jars/aws-java-sdk-kms-1.11.534.jar,\
	dags/jars/aws-java-sdk-s3-1.11.534.jar,\
	dags/jars/httpclient-4.5.3.jar,\
	dags/jars/joda-time-2.9.9.jar \
	dags/etl/spark_app.py


	spark-submit --master spark://spark-master:7077 \
	--deploy-mode client --driver-memory 2g \
	--num-executors 2 \
	--jars dags/jars/aws-java-sdk-1.11.534.jar,\
	dags/jars/aws-java-sdk-bundle-1.11.874.jar,\
	dags/jars/delta-core_2.12-1.0.0.jar,\
	dags/jars/hadoop-aws-3.2.0.jar \
	dags/etl/spark_app.py