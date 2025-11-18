import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime, timedelta

def scrape_wait_times(url):
    """
    Scrapes a queue-times.com calendar page for average
    and maximum wait times per ride.
    Returns a DataFrame with columns: Date, Ride, Average Wait Time (mins), Max Wait Time (mins)
    """

    # Always extract the date from the URL FIRST so we can use it in error messages
    url_parts = url.split('/')
    if len(url_parts) >= 7:
        year, month, day = url_parts[-3], url_parts[-2], url_parts[-1]
        date_str = f"{year}-{month}-{day}"
    else:
        date_str = datetime.now().strftime("%Y-%m-%d")

    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        response = requests.get(url, headers=headers, timeout=10)
        # This is where your 404 was being raised
        response.raise_for_status()

        soup = BeautifulSoup(response.text, 'html.parser')

        tables = soup.find_all('table', class_='table is-fullwidth')
        
        if not tables:
            print(f"Warning: Could not find tables for {date_str}")
            return None
        
        rides_data = {}
        
        for idx, table in enumerate(tables):
            tbody = table.find('tbody')
            if not tbody:
                continue
            
            rows = tbody.find_all('tr')
            
            for row in rows:
                cols = row.find_all('td')
                if len(cols) >= 2:
                    ride_link = cols[0].find('a')
                    ride_name = ride_link.get_text(strip=True) if ride_link else cols[0].get_text(strip=True)
                    
                    time_span = cols[1].find('span')
                    wait_time = time_span.get_text(strip=True) if time_span else cols[1].get_text(strip=True)
                    
                    if ride_name not in rides_data:
                        rides_data[ride_name] = {
                            'Date': date_str,
                            'Ride': ride_name,
                            'Average Wait Time (mins)': None,
                            'Max Wait Time (mins)': None
                        }
                    
                    if idx == 0:  # Average
                        rides_data[ride_name]['Average Wait Time (mins)'] = float(wait_time)
                    elif idx == 1:  # Max
                        rides_data[ride_name]['Max Wait Time (mins)'] = float(wait_time)
        
        if not rides_data:
            return None
        
        data = list(rides_data.values())
        df = pd.DataFrame(data)
        return df

    except requests.exceptions.HTTPError as e:
        # Specifically handle 404 as "date doesn't exist"
        if e.response is not None and e.response.status_code == 404:
            print(f"Date {date_str} does not exist on queue-times.com (404)")
        else:
            print(f"HTTP error for {date_str}: {e}")
        return None

    except requests.exceptions.RequestException as e:
        print(f"Request failed for {date_str}: {e}")
        return None

    except Exception as e:
        print(f"An error occurred for {date_str}: {e}")
        return None


def scrape_multiple_days(start_date, end_date, park_id=17):
    """
    Scrapes wait times for multiple days and returns a combined DataFrame.
    
    Args:
        start_date (str): Start date in format 'YYYY-MM-DD'
        end_date (str): End date in format 'YYYY-MM-DD'
        park_id (int): Park ID (default: 16 for Disneyland)
    
    Returns:
        pd.DataFrame: Combined data for all days
    """
    start = datetime.strptime(start_date, '%Y-%m-%d')
    end = datetime.strptime(end_date, '%Y-%m-%d')
    
    all_data = []
    current_date = start
    total_days = (end - start).days + 1
    
    print(f"Scraping wait times from {start_date} to {end_date} ({total_days} days)\n")
    
    day_count = 0
    while current_date <= end:
        day_count += 1
        year = current_date.strftime('%Y')
        month = current_date.strftime('%m')
        day = current_date.strftime('%d')
        
        url = f"https://queue-times.com/parks/{park_id}/calendar/{year}/{month}/{day}"
        print(f"[{day_count}/{total_days}] Scraping {current_date.strftime('%Y-%m-%d')}...", end=" ")
        
        df = scrape_wait_times(url)
        if df is not None:
            all_data.append(df)
            print(f"✓ ({len(df)} rides)")
        else:
            print("✗ (Failed)")
        
        current_date += timedelta(days=1)
    
    if all_data:
        combined_df = pd.concat(all_data, ignore_index=True)
        return combined_df
    else:
        return pd.DataFrame()


def save_to_csv(df, filename='wait_times_2015_2025_DCA.csv'):
    """
    Saves DataFrame to CSV file.
    
    Args:
        df (pd.DataFrame): DataFrame to save
        filename (str): Output filename
    """
    df.to_csv(filename, index=False)
    print(f"\n✓ Data saved to {filename}")
    print(f"  Total rows: {len(df)}")
    print(f"  Columns: {', '.join(df.columns)}")


if __name__ == "__main__":
    # Define the date range to scrape
    start_date = "2015-01-01"
    end_date = "2024-12-31"  # Change this to your desired end date
    
    # Scrape wait times for multiple days
    df = scrape_multiple_days(start_date, end_date)
    
    # Save to CSV
    if not df.empty:
        save_to_csv(df, 'wait_times.csv')
        print(f"\nPreview of data:")
        print(df.head(10))
    else:
        print("No data was scraped.")
