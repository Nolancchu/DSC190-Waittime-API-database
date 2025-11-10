import os
import requests
import pandas as pd
from datetime import datetime
import psycopg2

targets = {
    16: 'Disneyland', 
    17: 'Disney California Adventure', 
    42: 'Six Flags Magic Mountain',
    61: 'Knott\' Berry Farm', 
    66: 'Universal Studios Hollywood',
}

day_of_week_map = {
    0: "Monday",
    1: "Tuesday",
    2: "Wednesday",
    3: "Thursday",
    4: "Friday",
    5: "Saturday",
    6: "Sunday"
}

def main():
    rows = []  

    for park_id, park_name in targets.items():
        parks = requests.get(f"https://queue-times.com/parks/{park_id}/queue_times.json").json()
        
        for region in parks['lands']:
            for ride in region['rides']:
                ts = ride['last_updated']
                if ts is None:
                    continue 

                dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
                dow = day_of_week_map[dt.weekday()]

                row = {
                    "park": park_name,
                    "ride": ride["name"],
                    "wait_time": ride["wait_time"],
                    "day_of_week": dow,
                    "timestamp": dt,
                    "time": dt.time(),
                    "month": dt.month,
                    "year": dt.year,
                }
                rows.append(row)

    wait_time_data = pd.DataFrame(
        rows,
        columns=['park', 'ride', 'wait_time', 'day_of_week', 'timestamp', 'time', 'month', 'year']
    )

    insert_into_db(wait_time_data)


def insert_into_db(df):
    conn = psycopg2.connect(
        host=os.environ["DB_HOST"],
        dbname=os.environ["DB_NAME"],
        user=os.environ["DB_USER"],
        password=os.environ["DB_PASSWORD"],
        port=os.environ.get("DB_PORT", "5432"),
    )
    cur = conn.cursor()

    for _, row in df.iterrows():
        cur.execute(
            """
            INSERT INTO wait_times (park, ride, wait_time, day_of_week, timestamp, time, month, year)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                row["park"],
                row["ride"],
                row["wait_time"],
                row["day_of_week"],
                row["timestamp"],
                row["time"],
                row["month"],
                row["year"],
            ),
        )

    conn.commit()
    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
