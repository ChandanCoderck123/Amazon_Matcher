import pandas as pd
from sqlalchemy import create_engine
from thefuzz import fuzz
from tqdm import tqdm
import csv

# 1. DB CONNECTION
MYSQL_USER = 'Chandan'
MYSQL_PASSWORD = 'Chandan%40%234321'  # URL-encoded!
MYSQL_HOST = 'holistique-middleware.c9wdjmzy25ra.ap-south-1.rds.amazonaws.com'
MYSQL_DB = 'Amazon'

engine = create_engine(
    f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}/{MYSQL_DB}",
    pool_recycle=3600, pool_pre_ping=True
)

# 2. LOAD & AGGREGATE SEARCHTERM TABLE 
print("Loading SearchTerm table from MySQL...")
st_df = pd.read_sql("""
    SELECT
        id,
        `Customer_Search_Term`,
        `Campaign_Name`,
        `Match_Type`,
        `Impressions`,
        `Spend`,
        `Sales_14_Day_Total`,
        DATE_FORMAT(`Start_Date`, '%%Y-%%m') AS Month
    FROM SearchTerm
    WHERE `Start_Date` IS NOT NULL
""", engine)

st_agg = (
    st_df.groupby(['Month', 'Customer_Search_Term', 'Campaign_Name', 'Match_Type'], as_index=False)
    .agg({
        'Impressions': 'sum',
        'Spend': 'sum',
        'Sales_14_Day_Total': 'sum'
    })
)

# 3. LOAD & AGGREGATE SQP TABLE 
print("Loading SQP table from MySQL...")
sqp_df = pd.read_sql("""
    SELECT
        id,
        `Search Query`,
        `Search Query Volume`,
        `Impressions: Total Count`,
        `Impressions: Brand Count`,
        `Clicks: Total Count`,
        `Clicks: Brand Count`,
        DATE_FORMAT(`Reporting Date`, '%%Y-%%m') AS Month
    FROM SQP
    WHERE `Reporting Date` IS NOT NULL
""", engine)

sqp_agg = (
    sqp_df.groupby(['Month', 'Search Query'], as_index=False)
    .agg({
        'Search Query Volume': 'sum',
        'Impressions: Total Count': 'sum',
        'Impressions: Brand Count': 'sum',
        'Clicks: Total Count': 'sum',
        'Clicks: Brand Count': 'sum'
    })
)

# 4. MATCHING LOGIC & STREAM TO CSV 
months = sorted(set(st_agg['Month']) & set(sqp_agg['Month']))

output_file = "monthly_matched_searchterm_sqp_updated_finalv3.csv"
header = [
    'Month', 'Search Query', 'Search Query Volume (Sum)', 'Impressions: Total Count (Sum)',
    'Impressions: Brand Count (Sum)', 'Clicks: Total Count (Sum)', 'Clicks: Brand Count (Sum)',
    'Customer Search Term', 'Campaign Name', 'Match Type', 'Impressions (Sum)', 'Spend (Sum)',
    'Revenue (Sales_14_Day_Total Sum)', 'Match Score'
]

with open(output_file, 'w', newline='', encoding='utf-8') as f:
    writer = csv.DictWriter(f, fieldnames=header)
    writer.writeheader()

    for month in tqdm(months, desc='Month-wise Matching'):
        st_month = st_agg[st_agg['Month'] == month].reset_index(drop=True)
        sqp_month = sqp_agg[sqp_agg['Month'] == month].reset_index(drop=True)
        st_terms = st_month['Customer_Search_Term'].astype(str).tolist()

        for _, sqp_row in sqp_month.iterrows():
            search_query = str(sqp_row['Search Query'])
            first_word = search_query.split()[0]
            # filter for long first word
            if len(first_word) >= 10 and ' ' not in first_word:
                continue

            # Compute fuzzy match scores for all SearchTerms 
            matches = []
            for j, st_term in enumerate(st_terms):
                score = fuzz.token_sort_ratio(search_query, st_term)
                matches.append((st_term, score, j))

            # MAIN LOGIC UPDATE
            # Store all with score >70, else just the single best match
            filtered = [m for m in matches if m[1] > 70]
            if not filtered and matches:
                filtered = [max(matches, key=lambda x: x[1])]
            filtered = sorted(filtered, key=lambda x: x[1], reverse=True)

            for match_str, score, idx in filtered:
                st_row = st_month.iloc[idx]
                row = {
                    'Month': month,
                    'Search Query': search_query,
                    'Search Query Volume (Sum)': sqp_row['Search Query Volume'],
                    'Impressions: Total Count (Sum)': sqp_row['Impressions: Total Count'],
                    'Impressions: Brand Count (Sum)': sqp_row['Impressions: Brand Count'],
                    'Clicks: Total Count (Sum)': sqp_row['Clicks: Total Count'],
                    'Clicks: Brand Count (Sum)': sqp_row['Clicks: Brand Count'],
                    'Customer Search Term': st_row['Customer_Search_Term'],
                    'Campaign Name': st_row['Campaign_Name'],
                    'Match Type': st_row['Match_Type'],
                    'Impressions (Sum)': st_row['Impressions'],
                    'Spend (Sum)': st_row['Spend'],
                    'Revenue (Sales_14_Day_Total Sum)': st_row['Sales_14_Day_Total'],
                    'Match Score': int(score)
                }
                writer.writerow(row)
print(f"\nDone! Saved output to {output_file}")

