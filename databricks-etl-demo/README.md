
# Databricks ETL Demo

## 📌 Project Overview
This project showcases a complete ETL pipeline built in Databricks using Apache Spark and SQL. It demonstrates how to read raw data, apply structured transformations, and create optimized tables following the Medallion Architecture (Bronze, Silver, Gold). The goal is to highlight best practices in scalable data engineering.

## 🧱 Architecture
The ETL process follows the Medallion Architecture:
- **Bronze Layer**: Raw data ingestion.
- **Silver Layer**: Data cleansing and transformation.
- **Gold Layer**: Aggregated and business-ready tables.

## ⚙️ Technologies Used
- Databricks Community Edition
- Apache Spark (PySpark)
- SQL
- Delta Lake

## 🚀 Setup Instructions
1. Open [Databricks Community Edition](https://community.cloud.databricks.com/).
2. Create a new notebook and import the project code.
3. Run each cell step-by-step to execute the ETL pipeline.
4. Export the notebook as `.ipynb` to version it in GitHub.

## 📂 Project Structure
```
databricks-etl-demo/
├── notebooks/
│   ├── 01_ingest_bronze.ipynb
│   ├── 02_transform_silver.ipynb
│   └── 03_aggregate_gold.ipynb
├── lib/
│   ├── utils.py              # Reusable functions (e.g., reading, writing, validations)
│   └── config_loader.py      # Loading parameters, routes, configurations
├── data/
│   └── sample_data.csv       # Input data for testing
├── output/
│   ├── bronze/               # Data transformed into Bronze layer
│   ├── silver/               # Clean data in Silver layer
│   └── gold/                 # Final tables in Gold layer
├── tests/
│   └── test_utils.py         # Unit tests for lib/ functions
├── README.md
├── requirements.txt          # Project dependencies
└── .gitignore                # Files to exclude from version control
```

## 📈 Usage
- Run the Bronze notebook to ingest raw data.
- Run the Silver notebook to clean and transform.
- Run the Gold notebook to create final tables.

## 👤 Author
**Cristian Fernández Nieto**  
Data Engineering Enthusiast  
[GitHub Profile](https://github.com/CristianFernandez98)

## 📄 License
This project is licensed under the MIT License.
