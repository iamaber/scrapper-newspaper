import requests
from bs4 import BeautifulSoup
import pandas as pd
from newsplease import NewsPlease
from datetime import datetime, timedelta

def generate_date_list(start_date_str, end_date_str):
    """Generate a list of dates between start_date and end_date."""
    start_date = datetime.strptime(start_date_str, '%Y-%m-%d')
    end_date = datetime.strptime(end_date_str, '%Y-%m-%d')
    return [(start_date + timedelta(days=x)).strftime('%Y-%m-%d') for x in range((end_date - start_date).days + 1)]

def fetch_sitemap_urls(date_list):
    """Fetch URLs from the sitemap for each date in the date list."""
    locs = []
    dates = []
    for date_str in date_list:
        sitemap_url = f'https://www.prothomalo.com/sitemap/sitemap-daily-{date_str}.xml'
        try:
            source = requests.get(sitemap_url).text
            soup = BeautifulSoup(source, 'xml')
            for url in soup.find_all('url'):
                loc = url.find('loc').text
                lastmod = url.find('lastmod').text
                locs.append(loc)
                dates.append(lastmod)
        except requests.exceptions.RequestException as e:
            print(f"Error fetching sitemap for date {date_str}: {e}")
            continue
    return pd.DataFrame({'URL': locs, 'Last Modified Date': dates})

def scrape_articles(data_links):
    """Scrape articles from the URLs and return a DataFrame with the required information."""
    articles_data = []
    for index, row in data_links.iterrows():
        page = row['URL']
        try:
            article = NewsPlease.from_url(page)
            article_body = article.maintext
            date_published = article.date_publish
            headline = article.title
            articles_data.append({
                'Date Published': date_published,
                'Headline': headline,
                'Article Body': article_body,
                'Article Link': page,
                'Article Site': 'Prothom Alo'
            })
            print(f"Scraped article: {headline}")
        except Exception as e:
            print(f"Error scraping article at {page}: {e}")
            continue
    return pd.DataFrame(articles_data)

def scrap_prothomalo(start_date, end_date):    
    # Generate date list
    date_list = generate_date_list(start_date, end_date)
    
    # Fetch sitemap URLs
    data_links = fetch_sitemap_urls(date_list)
    
    # Scrape articles and get the DataFrame
    articles_df = scrape_articles(data_links)
    
    return articles_df
    

    
    
start_date = '2024-01-01'
end_date = '2024-06-27'

articles_df = scrap_prothomalo(start_date, end_date)
# Save to CSV
articles_df.to_csv('data.csv', index=False, encoding='utf-8')
    
# Load the dataset to verify
dataset = pd.read_csv("data.csv")
