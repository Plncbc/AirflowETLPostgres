#!/bin/bash
echo -e "AIRFLOW_UID=$(id -u)" > .env
echo COMPOSE_PROJECT_NAME=pet_project >> .env
sudo docker compose up airflow-init
sudo docker compose up -d

