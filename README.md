# Data Engineering ETL Pipeline Blueprint

A professional, modular Data Engineering ETL pipeline designed in Visual Studio Code. This project generates synthetic user data using **Faker**, cleans and transforms the dataset using **Pandas**, and loads the output into a **PostgreSQL** database running inside a **Docker** container. It finally uses Airflow for scheduling and orchestration

You can watch the youtube video for this source code here - https://youtu.be/9qnkuRFJxWY


## How to run the project
1. Install docker or docker desktop on your local machine
2. Clone or download this repo to your local machine
3. Inside the repo folder run this command: docker compose up -d 
4. Go to your docker dashboard or container logs and get the admin password
5. Navigate to your browser and bring up airflow UI: localhost:<port_number> such as 8080, 8082 etc
6. Log in to airflow UI then run your DAG

Note: You may need to generate and input your FERNET key in this variable AIRFLOW__CORE__FERNET_KEY=<YOUR_FERNET_KEY_HERE> or just delete the variable altogether