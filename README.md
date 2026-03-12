# Finance Data Pipeline

An **end-to-end Finance Data Engineering Pipeline** that ingests, transforms, and loads financial transaction data using modern tools and best practices. Designed to demonstrate real-world Data Engineering skills for portfolio purposes.

---

## 🚀 Pipeline Architecture


+-----------------+ +----------------+ +-----------------+
| Raw CSV Data | ---> | PySpark ETL | ---> | PostgreSQL DB |
| data_lake/raw/ | | (Transform) | | finance schema |
+-----------------+ +----------------+ +-----------------+
|
v
Airflow DAGs
(Orchestration &
Scheduling

--



## 🔧 Technologies Used

- **Python** – core scripting  
- **Apache Airflow** – DAG orchestration and workflow scheduling  
- **PySpark** – distributed data processing  
- **PostgreSQL** – relational database for processed data  
- **Docker & Docker Compose** – containerized environment  
- **Git & GitHub** – version control  

---

## 📂 Project Structure


finance-data-pipeline/
├── dags/ # Airflow DAGs
├── scripts/ # ETL scripts: ingest, transform, load
├── data_lake/ # Raw and processed data (ignored in Git)
├── docker-compose.yml # Docker setup for Airflow & Postgres
├── requirements.txt # Python dependencies
├── README.md
└── .gitignore


---

## 🔄 Pipeline Flow

1. **Ingest**  
   - Reads raw CSV transaction files from `data_lake/raw/`.  

2. **Transform**  
   - PySpark cleans, validates, and transforms the data.  
   - Handles schema validation, date formatting, and simple aggregations.  

3. **Load**  
   - Inserts transformed data into PostgreSQL `finance` schema.  
   - Designed a **Star Schema** to optimize queries and reporting:  
     - **Fact Table:** `transactions_fact`  
     - **Dimension Tables:** `customer_dim`, `account_dim`, `date_dim`  
   - This allows **efficient analytics and reporting**

4. **Orchestration**  
   - Airflow DAGs manage task dependencies, retries, and logging.  
   - DAG can be triggered manually or scheduled automatically.

5. **Containerization**  
   - Docker Compose spins up Airflow, PostgreSQL, and worker containers.  
   - Ensures reproducible and portable environment.

---

## ⚡ How to Run Locally

1. **Clone the repo:**

```bash
git clone https://github.com/Waleedarfan12/finance-data-pipeline.git
cd finance-data-pipeline

Build and start Docker containers:

docker-compose up --build

Access Airflow:

Webserver: http://localhost:8080

Username: airflow | Password: airflow123

Trigger the DAG finance_etl_pipeline manually or set the schedule.

