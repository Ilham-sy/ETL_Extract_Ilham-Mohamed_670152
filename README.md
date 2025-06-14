# ETL Extract Lab

Ilham Mohamed 
670152

## Description

This project demonstrates Full vs Incremental Extraction in an ETL pipeline using a synthetic sales dataset. The notebook simulates 50 sales records and extracts data accordingly.

## Structure
etl_extract.ipynb # My Jupyter notebook
user_sessions.csv # My generated dataset
last_extraction.txt # for incremental extraction
.gitignore # To exclude unneeded files
README.md # To explain my project

## Tools Used

- Python 3.x
- Pandas
- Jupyter Notebook

## How to Run

1. Clone this repository.
2. Ensure Python and Jupyter are installed.
3. Install required packages:
   ```
   pip install pandas jupyter
   ```
4. Launch the notebook:
   ```
   jupyter notebook etl_extract.ipynb
   ```
5. The notebook:
   - Generates `user_sessions.csv` (50 rows of data)
   - Performs Full Extraction
   - Performs Incremental Extraction using `last_extraction.txt`

## Features
 Data Pulled            
 Performance           
 Simplicity             
 Ideal Use Case       

# Extraction Methods
## Full Extraction
Pulls all rows from the dataset,
Easy to implement,
Slower on large datasets,
Ideal for small or test data

## Incremental Extraction
Pulls only new/updated rows,
Requires tracking with a timestamp,
Much faster and efficient,
Used in production ETL workflows

 ## Lab 4: Transformation Steps

This lab builds on Lab 3 by applying transformation logic to the ETL pipeline.

### Applied Transformations:
1. **Cleaning**: Removed duplicate records from both datasets.
2. **Enrichment**: Added a `session_duration_minutes` column.
3. **Structural**: Standardized `user_id` to uppercase format.

### Files Generated:
- `transformed_full.csv`
- `transformed_incremental.csv`