import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

def create_session():
    ###Create a session with retry strategy
    session = requests.Session()
    retries = Retry(
        total=5,
        backoff_factor=1,
        status_forcelist=[500, 502, 503, 504]
    )
    session.mount('http://', HTTPAdapter(max_retries=retries))
    session.mount('https://', HTTPAdapter(max_retries=retries))
    return session

def get_article_links(session, date, page):
    try:
        url = f"https://www.jugantor.com/archive?search=yes&headline=&date={date}&page={page}"
        response = session.get(url, timeout=10)
        soup = BeautifulSoup(response.text, 'html.parser')
        links = soup.find_all('a', class_='linkOverlay')
        return [link['href'] for link in links]
    except Exception as e:
        print(f"Error fetching article links for date {date}, page {page}: {e}")
        return []

def scrape_article_details(session, article_url):
    try:
        response = session.get(article_url, timeout=10)
        soup = BeautifulSoup(response.text, 'html.parser')

        # Extract headline
        headline = soup.find('h1', class_='desktopDetailHeadline marginT0')
        headline_text = headline.text.strip() if headline else "No headline found"

        # Extract publish date
        date_element = soup.find('div', class_='reporterName')
        date_published = datetime.now()  # Default to current date if not found
        if date_element:
            try:
                date_text = date_element.text.strip()
                # Add your date parsing logic here based on Jugantor's date format
                # date_published = datetime.strptime(date_text, 'your_format')
            except ValueError:
                pass

        # Extract body
        body = soup.find('div', class_='desktopDetailBody')
        body_text = ' '.join(p.text.strip() for p in body.find_all('p')) if body else "No content found"

        return {
            'Date Published': date_published,
            'Headline': headline_text,
            'Article Body': body_text,
            'Article Link': article_url,
            'Article Site': 'Jugantor'
        }
    except Exception as e:
        print(f"Error scraping article {article_url}: {e}")
        return None

def scrape_date_articles(session, date_str):
    articles_data = []
    page = 1
    
    while True:
        article_links = get_article_links(session, date_str, page)
        if not article_links:
            break

        # Use ThreadPoolExecutor for parallel article scraping
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = [
                executor.submit(scrape_article_details, session, link)
                for link in article_links
            ]
            
            for future in futures:
                result = future.result()
                if result:
                    articles_data.append(result)

        page += 1
        
    return articles_data

def scrap_jugantor(start_date, end_date):
    session = create_session()
    all_articles = []
    
    # Convert dates to datetime objects
    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt = datetime.strptime(end_date, "%Y-%m-%d")
    
    # Create list of dates to scrape
    dates = [
        (start_dt + timedelta(days=x)).strftime("%Y-%m-%d")
        for x in range((end_dt - start_dt).days + 1)
    ]
    
    # Use ThreadPoolExecutor for parallel date processing
    with ThreadPoolExecutor(max_workers=3) as executor:
        futures = [
            executor.submit(scrape_date_articles, session, date_str)
            for date_str in dates
        ]
        
        for future in futures:
            articles = future.result()
            all_articles.extend(articles)
    
    # Create DataFrame
    df = pd.DataFrame(all_articles)
    
    # Ensure all required columns are present
    required_columns = [
        'Date Published', 'Headline', 'Article Body', 
        'Article Link', 'Article Site'
    ]
    for col in required_columns:
        if col not in df.columns:
            df[col] = None
            
    return df

# # Example usage
# if __name__ == "__main__":
#     start_date = "2024-10-10"
#     end_date = "2024-10-10"
#     df = scrap_jugantor(start_date, end_date)
#     print(f"Total articles scraped: {len(df)}")