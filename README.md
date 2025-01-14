# Automated Data Pipeline for Bike Retail Chain
Demo project for skills demonstration

**About project:** This project runs the entire data cycle, transforming raw data into an analytical model and data marts for analysis using PostgreSQL and Apache Airflow. It is deployed in Docker for easy portability. Data mart is integrated with PowerBI for reporting and data exploration.

**RUN GUIDE:** 
the following scripts should be executed in the working directory

***auto***

Linux:

- ```./run_project.sh ```
  
Windows:

- ```./run_project.bat ```


***manual***
- ```echo -e "AIRFLOW_UID=$(id -u)" > .env ```
- ```sudo docker compose up airflow-init ```
- ``` sudo docker compose up ```

If everything is ok,
 
***postgresql connection:***
- adress: **localhost:5454**
- login: **admin**
- password: **123**

***airflow connection:***
- adress: **localhost:8080**
- login: **airflow**
- password: **airflow**

***dag in airflow must be started manually***

<details><summary><b>Structure:</b></summary>
 
![Project structure](images/pet_project_structure.jpg)

</details>

<details><summary><b>Screenshots:</b></summary>

![Project structure](images/airflow_graph.jpg)

<center>"airflow graph"</center>

![Project structure](images/sales_overview.jpg)

<center>"sales overviews"</center>

![Project structure](images/customer_segmentation.jpg)

<center>"customer segmentation"</center>

![Project structure](images/staff_perfomance.jpg)

<center>"staff perfomance"</center>

![Project structure](images/store_product_analysis.jpg)

<center>"store product analysis"</center>

</details>

**Technology stack:**
- PostgreSQL
- Python
- Airflow
- PowerBI
- Docker, Docker-compose
- Linux commands
- Git
