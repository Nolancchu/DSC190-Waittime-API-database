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
    try:
        # 1. Fetch the webpage content
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()  # Check for any request errors

        # 2. Parse the HTML with BeautifulSoup
        soup = BeautifulSoup(response.text, 'html.parser')

        # 3. Extract date from URL (format: /parks/16/calendar/2025/06/11)
        url_parts = url.split('/')
        if len(url_parts) >= 7:
            year, month, day = url_parts[-3], url_parts[-2], url_parts[-1]
            date_str = f"{year}-{month}-{day}"
        else:
            date_str = datetime.now().strftime("%Y-%m-%d")

        # 4. Find all tables with class "table is-fullwidth"
        tables = soup.find_all('table', class_='table is-fullwidth')
        
        if not tables:
            print(f"Warning: Could not find tables for {date_str}")
            return None
        
        # 5. Create a dictionary to store data by ride name
        rides_data = {}
        
        for idx, table in enumerate(tables):
            # Find all rows in tbody
            tbody = table.find('tbody')
            if not tbody:
                continue
            
            rows = tbody.find_all('tr')
            
            # Extract ride names and wait times
            for row in rows:
                cols = row.find_all('td')
                if len(cols) >= 2:
                    # Get ride name from the first column (extract text from anchor tag if present)
                    ride_link = cols[0].find('a')
                    ride_name = ride_link.get_text(strip=True) if ride_link else cols[0].get_text(strip=True)
                    
                    # Get wait time from the second column (extract from span if present)
                    time_span = cols[1].find('span')
                    wait_time = time_span.get_text(strip=True) if time_span else cols[1].get_text(strip=True)
                    
                    # Initialize ride entry if not exists
                    if ride_name not in rides_data:
                        rides_data[ride_name] = {
                            'Date': date_str,
                            'Ride': ride_name,
                            'Average Wait Time (mins)': None,
                            'Max Wait Time (mins)': None
                        }
                    
                    # Assign to appropriate column based on table number
                    if idx == 0:  # Table 1 is Average
                        rides_data[ride_name]['Average Wait Time (mins)'] = float(wait_time)
                    elif idx == 1:  # Table 2 is Max
                        rides_data[ride_name]['Max Wait Time (mins)'] = float(wait_time)
        
        if not rides_data:
            return None
        
        # 6. Convert dictionary to list of dictionaries for DataFrame
        data = list(rides_data.values())
        
        # 7. Create DataFrame
        df = pd.DataFrame(data)
        return df

    except requests.exceptions.RequestException as e:
        print(f"Error fetching URL for {date_str}: {e}")
        return None
    except Exception as e:
        print(f"An error occurred for {date_str}: {e}")
        return None


def scrape_multiple_days(start_date, end_date, park_id=16):
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


def save_to_csv(df, filename='wait_times.csv'):
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
    start_date = "2024-01-1"
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
