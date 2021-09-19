#!/bin/sh


set -o errexit
set -o nounset

airflow db init

airflow webserver -p 8000 & sleep 1 & airflow scheduler

