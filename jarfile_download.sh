cd airflow/dags/jars
echo ${PWD}
wget --no-verbose https://repo1.maven.org/maven2/org/mariadb/jdbc/mariadb-java-client/2.7.4/mariadb-java-client-2.7.4.jar
wget --no-verbose https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk/1.11.534/aws-java-sdk-1.11.534.jar
wget --no-verbose https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.874/aws-java-sdk-bundle-1.11.874.jar
wget --no-verbose https://repo1.maven.org/maven2/io/delta/delta-core_2.12/1.0.0/delta-core_2.12-1.0.0.jar
wget --no-verbose https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.2.0/hadoop-aws-3.2.0.jar
cd ../../..