import streamlit as st
import mysql.connector
import pandas as pd
import plotly.express as px
from io import StringIO

# Database configuration
DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': 'abir1234',
    'database': 'news_db'
}

def fetch_data_from_db(start_date, end_date, site_names):
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor(dictionary=True)
        site_placeholders = ', '.join(['%s'] * len(site_names))
        query = f"""
            SELECT * FROM articles
            WHERE DATE(articles.date_published) BETWEEN %s AND %s 
            AND article_site IN ({site_placeholders})
            ORDER BY date_published ASC 
        """
        cursor.execute(query, (start_date, end_date, *site_names))
        data = cursor.fetchall()
        conn.close()

        return pd.DataFrame(data)

    except mysql.connector.Error as e:
        st.error(f"Error fetching data: {e}")
        return pd.DataFrame()

def create_article_count_visualization(df):
    article_counts = df['article_site'].value_counts().reset_index()
    article_counts.columns = ['Site', 'Number of Articles']

    fig = px.bar(
        article_counts,
        x='Site',
        y='Number of Articles',
        title='Number of Articles by News Site',
        color='Site'
    )
    return fig

# Streamlit UI
st.title("News Article Scraper")

start_date = st.date_input("Start Date")
end_date = st.date_input("End Date")

# Multiple site selection
available_sites = ["Prothom Alo", "Bangladesh Protidin", "Jugantor"]
selected_sites = st.multiselect(
    "Select News Site(s)",
    available_sites,
    default=available_sites[0]
)

# Button to fetch data
if st.button("Fetch Data"):
    if selected_sites:
        df = fetch_data_from_db(start_date, end_date, selected_sites)
        if not df.empty:
            # Show total number of articles
            st.metric("Total Articles", len(df))
            st.dataframe(df)
            
            # Option to download as CSV
            csv = df.to_csv(index=False)
            st.download_button(
                label="Download CSV",
                data=csv,
                file_name=f"news_articles_{start_date}_{end_date}.csv",
                mime="text/csv"
            )
            st.plotly_chart(create_article_count_visualization(df))
        else:
            st.warning("No data found for the selected criteria.")
    else:
        st.warning("Please select at least one news site.")