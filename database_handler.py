import mysql.connector
from mysql.connector import Error
from datetime import datetime

class ArticleDatabase:
    def __init__(self, host, user, password, database):
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.connection = None
        
    def connect(self):
        try:
            self.connection = mysql.connector.connect(
                host=self.host,
                user=self.user,
                password=self.password,
                database=self.database
            )
            print("Successfully connected to the database")
        except Error as e:
            print(f"Error connecting to MySQL Database: {e}")
            
    def create_tables(self):
        try:
            cursor = self.connection.cursor()
            
            # Create articles table
            create_articles_table = """
            CREATE TABLE IF NOT EXISTS articles (
                id INT AUTO_INCREMENT PRIMARY KEY,
                date_published DATETIME,
                headline VARCHAR(500),
                article_body TEXT,
                article_link VARCHAR(500) UNIQUE,
                article_site VARCHAR(100),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            ) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
            """
            
            cursor.execute(create_articles_table)
            self.connection.commit()
            print("Tables created successfully")
            
        except Error as e:
            print(f"Error creating tables: {e}")
        finally:
            cursor.close()
            
    def store_articles(self, articles_df, source='Unknown'):
        try:
            cursor = self.connection.cursor()
            
            for _, row in articles_df.iterrows():
                # Convert date string to datetime object if it's a string
                date_published = row['Date Published']
                if isinstance(date_published, str):
                    try:
                        date_published = datetime.strptime(date_published, '%Y-%m-%d %H:%M:%S')
                    except ValueError:
                        # Try alternative date formats if the first one fails
                        try:
                            date_published = datetime.strptime(date_published, '%Y-%m-%d')
                        except ValueError:
                            print(f"Warning: Could not parse date {date_published}, using current datetime")
                            date_published = datetime.now()
                
                # Insert query with parameterized values
                insert_query = """
                INSERT IGNORE INTO articles 
                (date_published, headline, article_body, article_link, article_site)
                VALUES (%s, %s, %s, %s, %s)
                """
    
                values = (
                    date_published,
                    row['Headline'],
                    row['Article Body'],
                    row['Article Link'],
                    source
                )
                
                try:
                    cursor.execute(insert_query, values)
                except Error as e:
                    print(f"Error inserting article {row['Headline']}: {e}")
                    continue
                
            self.connection.commit()
            print(f"Successfully stored {len(articles_df)} articles in the database")
            
        except Error as e:
            print(f"Error storing articles: {e}")
        finally:
            cursor.close()
            
    def close_connection(self):
        if self.connection and self.connection.is_connected():
            self.connection.close()
            print("Database connection closed")
            
    def __enter__(self):
        self.connect()
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close_connection()