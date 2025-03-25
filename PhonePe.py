import os
import json
import mysql.connector
import pandas as pd
import streamlit as st
import plotly.express as px
from git import Repo

# Step 1: Clone the GitHub repository
github_url = "https://github.com/PhonePe/pulse.git"
repo_path = "phonepe_data"
if not os.path.exists(repo_path):
    Repo.clone_from(github_url, repo_path)
    print("Repository cloned successfully.")
else:
    print("Repository already exists.")

# Step 2: Connect to MySQL
db_config = {
    "host": "localhost",
    "user": "root",  # Change as per your MySQL credentials
    "password": "Greninja123@",  # Change your password
    "database": "phonepe_db"
}

db = mysql.connector.connect(**db_config)
cursor = db.cursor()

# Step 3: Create tables
cursor.execute("""
    CREATE TABLE IF NOT EXISTS aggregated_transaction (
        id INT AUTO_INCREMENT PRIMARY KEY,
        state VARCHAR(100),
        year INT,
        quarter INT,
        transaction_type VARCHAR(50),
        transaction_count BIGINT,  # Changed from INT to BIGINT
        transaction_amount FLOAT
    )
""")
db.commit()

# Step 4: Load JSON data into MySQL

data_path = os.path.join(repo_path, "data", "aggregated", "transaction", "country", "india", "state")

for state in os.listdir(data_path):
    state_path = os.path.join(data_path, state)
    for year in os.listdir(state_path):
        year_path = os.path.join(state_path, year)
        for file in os.listdir(year_path):
            if file.endswith(".json"):
                with open(os.path.join(year_path, file), "r") as f:
                    data = json.load(f)
                    quarter = int(file.split(".")[0])
                    for transaction in data["data"].get("transactionData", []):
                        transaction_type = transaction["name"]
                        transaction_count = transaction["paymentInstruments"][0]["count"]
                        transaction_amount = transaction["paymentInstruments"][0]["amount"]
                        
                        cursor.execute("""
                            INSERT INTO aggregated_transaction (state, year, quarter, transaction_type, transaction_count, transaction_amount)
                            VALUES (%s, %s, %s, %s, %s, %s)
                        """, (state, year, quarter, transaction_type, transaction_count, transaction_amount))
                        db.commit()

print("Data loading completed.")

# Step 5: Streamlit Dashboard
st.title("📊 PhonePe Business Insights Dashboard")

# Dropdown to select a business case study
case_study = st.selectbox("Select a Business Case Study", [
    "Decoding Transaction Dynamics",
    "Device Dominance and User Engagement",
    "Transaction Analysis for Market Expansion",
    "User Registration Analysis",
    "Transaction Analysis Across States and Districts"
])

# SQL Queries for Different Case Studies
queries = {
    "Decoding Transaction Dynamics": """
        SELECT state, SUM(transaction_count) AS total_transactions, SUM(transaction_amount) AS total_amount
        FROM aggregated_transaction GROUP BY state ORDER BY total_amount DESC;
    """,
    "Device Dominance and User Engagement": """
        SELECT transaction_type, SUM(transaction_count) AS total_transactions
        FROM aggregated_transaction GROUP BY transaction_type ORDER BY total_transactions DESC;
    """,
    "Transaction Analysis for Market Expansion": """
        SELECT year, quarter, SUM(transaction_count) AS total_transactions, SUM(transaction_amount) AS total_amount
        FROM aggregated_transaction GROUP BY year, quarter ORDER BY year, quarter;
    """,
    "User Registration Analysis": """
        SELECT state, COUNT(DISTINCT id) AS total_users
        FROM aggregated_transaction GROUP BY state ORDER BY total_users DESC;
    """,
    "Transaction Analysis Across States and Districts": """
        SELECT state, SUM(transaction_count) AS total_transactions, SUM(transaction_amount) AS total_amount
        FROM aggregated_transaction GROUP BY state ORDER BY total_transactions DESC;
    """
}

# Execute selected query
cursor.execute(queries[case_study])
results = cursor.fetchall()
columns = [desc[0] for desc in cursor.description]
df = pd.DataFrame(results, columns=columns)

# Display data and visualization
st.write(f"### {case_study}")
st.dataframe(df)

if not df.empty:
    fig = px.bar(df, x=df.columns[0], y=df.columns[1], title=f"{case_study}")
    st.plotly_chart(fig)

cursor.close()
db.close()
print("Streamlit Dashboard Ready.")
