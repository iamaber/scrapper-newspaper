from database_handler import ArticleDatabase
from scrappers.prothomalo import scrap_prothomalo
from scrappers.bangladeshProtidin import scrap_bangladeshProtidin
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime, timedelta
import pandas as pd

# Database configuration
DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': 'abir1234',
    'database': 'news_db'
}

def split_date_range(start_date, end_date, chunks):
    """Split the date range into smaller chunks for parallel processing"""
    start = datetime.strptime(start_date, '%Y-%m-%d')
    end = datetime.strptime(end_date, '%Y-%m-%d')
    total_days = (end - start).days + 1
    chunk_size = max(1, total_days // chunks)
    
    date_ranges = []
    current_start = start
    while current_start <= end:
        current_end = min(current_start + timedelta(days=chunk_size - 1), end)
        date_ranges.append((
            current_start.strftime('%Y-%m-%d'),
            current_end.strftime('%Y-%m-%d')
        ))
        current_start = current_end + timedelta(days=1)
    
    return date_ranges

def scrape_source(source_func, start_date, end_date, source_name):
    """Scrape articles from a specific source for a date range"""
    try:
        df = source_func(start_date, end_date)
        return {
            'source': source_name,
            'data': df,
            'start_date': start_date,
            'end_date': end_date,
            'count': len(df) if df is not None else 0
        }
    except Exception as e:
        print(f"Error scraping {source_name} for dates {start_date} to {end_date}: {e}")
        return {
            'source': source_name,
            'data': pd.DataFrame(),
            'start_date': start_date,
            'end_date': end_date,
            'count': 0
        }

def parallel_scrape_and_store(start_date, end_date, max_workers=4, chunks_per_source=3):
    """Scrape multiple sources in parallel and store results"""
    # Define scraping functions and their names
    scrapers = [
        (scrap_prothomalo, 'Prothom Alo'),
        (scrap_bangladeshProtidin, 'Bangladesh Protidin')
    ]
    
    # Split date range into chunks for each source
    date_ranges = split_date_range(start_date, end_date, chunks_per_source)
    
    # Create all scraping tasks
    scraping_tasks = [
        (scraper_func, date_range[0], date_range[1], scraper_name)
        for scraper_func, scraper_name in scrapers
        for date_range in date_ranges
    ]
    
    results = {name: pd.DataFrame() for _, name in scrapers}
    
    # Execute scraping tasks in parallel
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        # Submit all tasks
        future_to_task = {
            executor.submit(scrape_source, *task): task
            for task in scraping_tasks
        }
        
        # Process completed tasks
        for future in as_completed(future_to_task):
            result = future.result()
            source_name = result['source']
            
            if not result['data'].empty:
                if results[source_name].empty:
                    results[source_name] = result['data']
                else:
                    results[source_name] = pd.concat([results[source_name], result['data']], ignore_index=True)
                
                print(f"Completed scraping {result['count']} articles from {source_name} "
                      f"({result['start_date']} to {result['end_date']})")
    
    # Store results in database
    articles_stored = 0
    with ArticleDatabase(**DB_CONFIG) as db:
        db.create_tables()
        for source, df in results.items():
            if not df.empty:
                print(f"\nStoring {len(df)} articles from {source}...")
                db.store_articles(df, source=source)
                articles_stored += len(df)
    
    return {source: len(df) for source, df in results.items()}

def main():
    start_date = '2024-06-01'
    end_date = '2024-06-01'
    
    print(f"Starting parallel scraping from {start_date} to {end_date}")
    start_time = datetime.now()
    
    # Run the parallel scraping and storing process
    results = parallel_scrape_and_store(
        start_date, 
        end_date,
        max_workers=5,  # Adjust based on your CPU cores
        chunks_per_source=3  # Adjust based on your date range
    )
    
    end_time = datetime.now()
    duration = end_time - start_time
    
    print("\nScraping completed!")
    print(f"Total time taken: {duration}")
    print("\nSummary of articles scraped:")
    for source, count in results.items():
        print(f"{source}: {count} articles")

if __name__ == "__main__":
    main()