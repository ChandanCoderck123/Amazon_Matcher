import pandas as pd
import mysql.connector

# Load the CSV
df = pd.read_csv('SearchTerm (4) mar 25.csv')

# Add the 'month' column
df['month'] = 'mar 25'

# Connect to MySQL
conn = mysql.connector.connect(
    host='holistique-middleware.c9wdjmzy25ra.ap-south-1.rds.amazonaws.com',
    user='Chandan',
    password='Chandan@#4321',
    database='Amazon'
)
cursor = conn.cursor()

# Drop table if exists
cursor.execute("DROP TABLE IF EXISTS Search_Term")

# Dynamically generate table schema from CSV
columns = df.columns.drop('month')  # exclude manually added 'month' for now
column_definitions = ",\n    ".join([f"`{col}` TEXT" for col in columns])
create_table_query = f"""
CREATE TABLE Search_Term (
    id INT AUTO_INCREMENT PRIMARY KEY,
    {column_definitions},
    `month` VARCHAR(10)
)
"""
cursor.execute(create_table_query)

# Insert data using executemany (faster)
values = [tuple(row) for row in df.itertuples(index=False)]
column_names = ', '.join([f"`{col}`" for col in df.columns])
placeholders = ', '.join(['%s'] * len(df.columns))
insert_query = f"INSERT INTO Search_Term ({column_names}) VALUES ({placeholders})"
cursor.executemany(insert_query, values)

# Commit and close
conn.commit()
cursor.close()
conn.close()

print("✅ Upload completed successfully.")
