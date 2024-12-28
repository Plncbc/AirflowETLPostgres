#!/bin/bash
echo -e "AIRFLOW_UID=$(id -u)" > .env
sudo docker compose up airflow-init
sudo docker compose up -d

