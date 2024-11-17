import requests
from bs4 import BeautifulSoup
import pandas as pd
from newsplease import NewsPlease
from datetime import datetime, timedelta
import csv

# Function to generate a list of dates in the given range
def generate_date_list(start_date_str, end_date_str):
    start_date = datetime.strptime(start_date_str, '%Y-%m-%d')
    end_date = datetime.strptime(end_date_str, '%Y-%m-%d')
    return [(start_date + timedelta(days=x)).strftime('%Y-%m-%d') for x in range((end_date - start_date).days + 1)]

# Function to fetch URLs from sitemap
def fetch_sitemap_urls(date_list):
    locs, dates = [], []
    for date_str in date_list:
        sitemap_url = f'https://www.bd-pratidin.com/daily-sitemap/{date_str}/sitemap.xml'
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

# Function to scrape article data
def scrape_articles(data_links):
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
                'Article Site': 'Bangladesh Protidin'
            })
            print(f"Scraped article: {headline}")
        except Exception as e:
            print(f"Error scraping article at {page}: {e}")
            continue
    return pd.DataFrame(articles_data)


def scrap_bangladeshProtidin(start_date, end_date):
    date_list = generate_date_list(start_date, end_date)
    # Fetch sitemap URLs
    data_links = fetch_sitemap_urls(date_list)
    # Scrape articles and return DataFrame
    articles_df = scrape_articles(data_links)

    return articles_df

# # Function to write the scraped data to CSV
# def save_to_csv(data_frame, filename):
#     data_frame.to_csv(filename, index=False, encoding='utf-8')
#     print(f"Data saved to {filename}")

# # Example usage
# if __name__ == "__main__":
#     start_date = '2024-06-01'
#     end_date = '2024-06-01'

#     # Scrape data from Prothom Alo
#     articles_df = scrap_bangladeshProtidin(start_date, end_date)

#     # Save to CSV
#     save_to_csv(articles_df, 'data.csv')
