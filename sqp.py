import pandas as pd
import mysql.connector

# Read the CSV
df = pd.read_csv(r'C:\Users\Chandan Kumar\OneDrive - Work Store Limited\Documents\MySQL\Mysqlupload\SQP 2025 April.csv')

# Add the 'month' column
df['month'] = 'apr 25'

# Connect to MySQL
conn = mysql.connector.connect(
    host='',
    user='',
    password='',
    database='Amazon'
)
cursor = conn.cursor()

# Dynamically create table: all columns as TEXT for max compatibility
column_defs = ',\n    '.join([f"`{col}` TEXT" for col in df.columns])
create_table_query = f"""
CREATE TABLE SQP (
    id INT AUTO_INCREMENT PRIMARY KEY,
    {column_defs}
)
"""
cursor.execute(create_table_query)

# Insert data
values = [tuple(row) for row in df.itertuples(index=False)]
col_names = ', '.join([f"`{col}`" for col in df.columns])
placeholders = ', '.join(['%s'] * len(df.columns))
insert_query = f"INSERT INTO SQP ({col_names}) VALUES ({placeholders})"
cursor.executemany(insert_query, values)

conn.commit()
cursor.close()
conn.close()

print("SQP upload done!")

