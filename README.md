# ETL Extract Lab

Ilham Mohamed 
ID-670152

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
This notebook demonstrates how to transform the extracted data by cleaning, enriching, and structuring it for further analysis or loading into a database.  
### Transformation Steps:
1. **Cleaning**: Remove duplicate records from both datasets.  
2. **Enrichment**: Add a `session_duration_minutes` column to both datasets.
3. **Structural**: Standardize `user_id` to uppercase format.  



### Files Generated:
- `transformed_full.csv`
- `transformed_incremental.csv`

## Lab 5 – Load
# ETL Load Lab 
This lab builds on the previous transformation steps by loading the transformed data into SQLite databases. 
The notebook demonstrates how to load both full and incremental datasets into separate SQLite databases, allowing for efficient data management and querying.

### Loading Method Used:
SQLite database files for both full and incremental datasets.  

### Steps:
1. Loaded `transformed_full.csv` into `full_data.db`. 
2. Loaded `transformed_incremental.csv` into `incremental_data.db`.
3. Displayed a sample of the loaded data.

```python
import pandas as pd  
import sqlite3
from pathlib import Path
# Load transformed data into SQLite database 
output_dir = Path("output")
output_dir.mkdir(exist_ok=True)  
df_full = pd.read_csv(output_dir / "transformed_full.csv")
df_incremental = pd.read_csv(output_dir / "transformed_incremental.csv")

# Load full transformed data into SQLite database
conn_full = sqlite3.connect(output_dir / "full_data.db")
df_full.to_sql("full_data", conn_full, if_exists="replace", index=False)
conn_full.close()

# Load incremental transformed data into SQLite database
conn_incremental = sqlite3.connect(output_dir / "incremental_data.db")
df_incremental.to_sql("incremental_data", conn_incremental, if_exists="replace", index=False)
conn_incremental.close()

# Display a sample of the loaded data
conn_full = sqlite3.connect(output_dir / "full_data.db")    
print("Full Data Sample:")
display(pd.read_sql("SELECT * FROM full_data LIMIT 5", conn_full))
conn_incremental = sqlite3.connect(output_dir / "incremental_data.db")
print("Incremental Data Sample:")      
display(pd.read_sql("SELECT * FROM incremental_data LIMIT 5", conn_incremental))
```      

### Sample Table Schema (incremental_data):
```sql      
CREATE TABLE incremental_data (
    session_id TEXT PRIMARY KEY,
    user_id TEXT,
    device_type TEXT,
    start_time TEXT,
    end_time TEXT,
    session_duration_minutes REAL
);
```

### Sample Table Schema (full_data):
```sql
CREATE TABLE full_data (
    session_id TEXT PRIMARY KEY,
    user_id TEXT,
    device_type TEXT,
    start_time TEXT,
    end_time TEXT,
    session_duration_minutes REAL
);
```

### Sample Data
```sql   
SELECT * FROM full_data LIMIT 5;
SELECT * FROM incremental_data LIMIT 5;         
```   

### Conclusion
This lab successfully demonstrates the loading of transformed data into SQLite databases, showcasing both full and incremental datasets and their respective schemas. The use of SQLite allows for efficient querying and management of the data, making it suitable for further analysis or reporting.  



