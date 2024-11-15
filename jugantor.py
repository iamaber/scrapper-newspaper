import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime, timedelta

# Function to get all article links for a specific date
def get_article_links(date):
    url = f"https://www.jugantor.com/archive/{date}"
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')
    
    # Find all article links
    links = soup.find_all('a', class_='text-decoration-none text-body')
    article_links = [link['href'] for link in links]
    return article_links

# Function to scrape the details of an article
def scrape_article_details(article_url):
    response = requests.get(article_url)
    soup = BeautifulSoup(response.text, 'html.parser')
    
    # Extract the headline
    headline = soup.find('h3', class_='font-weight-bolder').text.strip()
    
    # Extract the writer's name if available
    writer = soup.find('span', class_='font-weight-bold text-dark')
    writer = writer.text.strip() if writer else "Unknown"
    
    # Extract the main body of the article
    body = soup.find('div', class_='IfTxty news-element-text text-justify my-2 pr-md-4 text-break')
    body_text = ' '.join(p.text.strip() for p in body.find_all('p'))
    
    return headline, writer, body_text

# Function to iterate over the date range and scrape articles
def scrape_jugantor(start_date, end_date):
    # Create a list to store article data
    articles_data = []
    
    # Convert start and end dates to datetime objects
    start_dt = datetime.strptime(start_date, "%Y/%m/%d")
    end_dt = datetime.strptime(end_date, "%Y/%m/%d")
    
    # Loop over each date in the range
    current_dt = start_dt
    while current_dt <= end_dt:
        date_str = current_dt.strftime("%Y/%m/%d")
        print(f"Scraping articles for date: {date_str}")
        
        # Get article links for the current date
        article_links = get_article_links(date_str)
        
        # Scrape details for each article
        for link in article_links:
            headline, writer, body_text = scrape_article_details(link)
            articles_data.append({
                'Date': date_str,
                'Headline': headline,
                'Writer': writer,
                'Body': body_text
            })
        
        # Move to the next date
        current_dt += timedelta(days=1)
    
    # Create a DataFrame from the collected data
    df = pd.DataFrame(articles_data)
    return df

# Example usage
start_date = "2024/11/10"
end_date = "2024/11/11"
df = scrape_jugantor(start_date, end_date)
print(df)

# Save the DataFrame to a CSV file if needed
df.to_csv("jugantor_articles.csv", index=False)
