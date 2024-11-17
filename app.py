import streamlit as st
import mysql.connector
import pandas as pd
from datetime import datetime, timedelta
import threading
import time
from sqlalchemy import create_engine
import traceback

from jugantor import scrape_jugantor

# Database configuration
DB_CONFIG = {
    'host': 'localhost',
    'user': 'newsuser',
    'password': 'abir1234',
    'database': 'news_db'
}

# Create SQLAlchemy engine
def get_database_engine():
    return create_engine(
        f"mysql+mysqlconnector://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}/{DB_CONFIG['database']}"
    )

def init_db():
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS news_articles (
                id INT AUTO_INCREMENT PRIMARY KEY,
                source VARCHAR(50),
                headline TEXT,
                writer VARCHAR(255),
                content TEXT,
                publication_date DATE,
                scrape_date DATETIME,
                url TEXT
            )
        """)
        conn.commit()
        return conn, cursor
    except Exception as e:
        st.error(f"Database connection error: {str(e)}")
        return None, None

def save_to_db(df, source, conn, cursor):
    try:
        for _, row in df.iterrows():
            cursor.execute("""
                INSERT INTO news_articles 
                (source, headline, writer, content, publication_date, scrape_date)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (
                source,
                row['Headline'],
                row['Writer'],
                row['Body'],
                datetime.strptime(row['Date'], "%Y/%m/%d").date(),
                datetime.now()
            ))
        conn.commit()
        return True
    except Exception as e:
        st.error(f"Error saving to database: {str(e)}")
        return False

def fetch_from_db(source, start_date, end_date):
    try:
        engine = get_database_engine()
        query = """
            SELECT * FROM news_articles 
            WHERE source = %s 
            AND publication_date BETWEEN %s AND %s
        """
        return pd.read_sql_query(query, engine, params=(source, start_date, end_date))
    except Exception as e:
        st.error(f"Error fetching from database: {str(e)}")
        return pd.DataFrame()

def schedule_scraping():
    while True:
        try:
            conn, cursor = init_db()
            if conn and cursor:
                end_date = datetime.now().strftime("%Y/%m/%d")
                start_date = (datetime.now() - pd.Timedelta(days=2)).strftime("%Y/%m/%d")
                
                df = scrape_jugantor(start_date, end_date)
                if not df.empty:
                    save_to_db(df, 'jugantor', conn, cursor)
                conn.close()
            
        except Exception as e:
            print(f"Scheduling error: {str(e)}")
            
        time.sleep(2 * 24 * 60 * 60)  # Wait for 2 days

def scrape_and_save(source, start_date, end_date):
    try:
        if source == 'jugantor':
            start_date_str = start_date.strftime("%Y/%m/%d")
            end_date_str = end_date.strftime("%Y/%m/%d")
            
            with st.spinner('Scraping news articles...'):
                df = scrape_jugantor(start_date_str, end_date_str)
                
                if df is None or df.empty:
                    st.warning("No articles found for the selected date range.")
                    return pd.DataFrame()
                
                conn, cursor = init_db()
                if conn and cursor:
                    if save_to_db(df, source, conn, cursor):
                        st.success("Data scraped and saved to database!")
                        conn.close()
                        # Fetch the newly saved data
                        return fetch_from_db(source, start_date, end_date)
                    else:
                        st.error("Failed to save data to database")
                        conn.close()
                        return pd.DataFrame()
        return pd.DataFrame()
    except Exception as e:
        st.error(f"Error during scraping: {str(e)}")
        st.error(f"Detailed error: {traceback.format_exc()}")
        return pd.DataFrame()

def main():
    st.title("Bengali News Scraper")
    
    sources = ['jugantor']
    selected_source = st.selectbox("Select News Portal", sources)
    
    col1, col2 = st.columns(2)
    with col1:
        start_date = st.date_input("Start Date")
    with col2:
        end_date = st.date_input("End Date")
    
    if st.button("Fetch News"):
        try:
            # First try to fetch from database
            df = fetch_from_db(selected_source, start_date, end_date)
            
            # If no data in database, scrape it
            if df.empty:
                st.info("Data not found in database. Scraping from website...")
                df = scrape_and_save(selected_source, start_date, end_date)
            
            # Display the data
            if not df.empty:
                st.write(f"Found {len(df)} articles")
                
                # Show article previews
                for _, row in df.iterrows():
                    with st.expander(f"{row['headline']} - {row['publication_date']}"):
                        st.write(f"Writer: {row['writer']}")
                        st.write("Content Preview:")
                        st.write(row['content'][:300] + "..." if len(row['content']) > 300 else row['content'])
                
                # Export option
                csv = df.to_csv(index=False)
                st.download_button(
                    label="Download as CSV",
                    data=csv,
                    file_name=f"{selected_source}_news_{start_date}_{end_date}.csv",
                    mime="text/csv"
                )
            else:
                st.warning("No data found for the selected date range")
            
        except Exception as e:
            st.error(f"Error: {str(e)}")
            st.error(f"Detailed error: {traceback.format_exc()}")

if __name__ == "__main__":
    # Start the scheduler thread
    scheduler_thread = threading.Thread(target=schedule_scraping, daemon=True)
    scheduler_thread.start()
    main()