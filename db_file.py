from database_handler import ArticleDatabase
from scrappers.prothomalo import scrap_prothomalo

# Database configuration
DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': 'abir1234',
    'database': 'news_db'
}

def main():
    start_date = '2024-06-01'
    end_date = '2024-06-01'
    
    # Scrape articles
    articles_df = scrap_prothomalo(start_date, end_date)
    
    # Store in database using context manager
    with ArticleDatabase(**DB_CONFIG) as db:
        db.create_tables()
        db.store_articles(articles_df)

if __name__ == "__main__":
    main()