@echo off

echo AIRFLOW_UID=1000 > .env
echo COMPOSE_PROJECT_NAME=pet_project >> .env
docker compose up airflow-init
docker compose up -d