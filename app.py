import streamlit as st
import mysql.connector
import pandas as pd
from io import StringIO

# Database configuration
DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': 'abir1234',
    'database': 'news_db'
}

def fetch_data_from_db(start_date, end_date, site_name):
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor(dictionary=True)

        query = """
            SELECT * FROM articles
            WHERE DATE(articles.date_published) BETWEEN %s AND %s AND article_site = %s
            ORDER BY date_published ASC 
        """
        cursor.execute(query, (start_date, end_date, site_name))
        data = cursor.fetchall()
        conn.close()

        return pd.DataFrame(data)

    except mysql.connector.Error as e:
        st.error(f"Error fetching data: {e}")
        return pd.DataFrame()

# Streamlit UI
st.title("News Article Scraper")

# User inputs for start date, end date, and site name
start_date = st.date_input("Start Date")
end_date = st.date_input("End Date")
site_name = st.selectbox("Select News Site", ["Prothom Alo", "Bangladesh Protidin"])

# Button to fetch data
if st.button("Fetch Data"):
    df = fetch_data_from_db(start_date, end_date, site_name)
    if not df.empty:
        st.dataframe(df)
        # Option to download as CSV
        csv = df.to_csv(index=False)
        st.download_button(
            label="Download CSV",
            data=csv,
            file_name=f"news_articles_{start_date}_{end_date}.csv",
            mime="text/csv"
        )
    else:
        st.warning("No data found for the selected criteria.")
