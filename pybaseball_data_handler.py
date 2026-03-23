import os
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime, timedelta
from pybaseball import statcast
import glob


def fetch_statcast_to_parquet(start_date_str, end_date_str, batch_days=14, output_name="data/full_2025_season.parquet"):
    if not os.path.exists('data'): os.makedirs('data')
    
    start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
    end_date = datetime.strptime(end_date_str, "%Y-%m-%d")
    current_start = start_date
    
    writer = None
    total_rows = 0

    while current_start < end_date:
        # Calculate the end of this specific batch
        current_end = min(current_start + timedelta(days=batch_days - 1), end_date)
        s_str, e_str = current_start.strftime("%Y-%m-%d"), current_end.strftime("%Y-%m-%d")
        
        success = False
        for attempt in range(2): # Quick human-style retry logic
            try:
                print(f"Fetching {s_str} to {e_str} (Attempt {attempt+1})...")
                df = statcast(start_dt=s_str, end_dt=e_str)
                
                if df is not None and not df.empty:
                    table = pa.Table.from_pandas(df)
                    
                    if writer is None:
                        # Initialize the file with the schema from the first successful batch
                        writer = pq.ParquetWriter(output_name, table.schema)
                    
                    writer.write_table(table)
                    batch_len = len(df)
                    total_rows += batch_len
                    print(f"   Stored {batch_len:,} rows. (Total: {total_rows:,})")
                    success = True
                    break 
                else:
                    print(f"   No games found in this range.")
                    success = True
                    break
                    
            except Exception as e:
                print(f"   Snagged an error: {e}")
                if attempt == 0:
                    print("   Waiting 5s before retrying...")
                    time.sleep(5)
                else:
                    print(f"   !! Skipping batch {s_str} after two tries.")

        # Move the window forward by exactly the batch size
        current_start += timedelta(days=batch_days)

    if writer:
        writer.close()
        print(f"\nFinished! Total rows saved: {total_rows:,}")
        print(f"File location: {output_name}")
    else:
        print("\nNo data was ever successfully fetched.")


def get_team_vector_from_parquet(team_abbr, parquet_path=None):
    # Search for the most recent parquet file instead of CSV
    if parquet_path is None:
        list_of_files = glob.glob('data/*.parquet')
        if not list_of_files:
            raise FileNotFoundError("No Parquet files found. Run the new fetcher first.")
        parquet_path = max(list_of_files, key=os.path.getctime)
    
    print(f"Fast-loading {team_abbr} from {parquet_path}...")

    # KEY CHANGE: We only load the 4 columns needed for the math.
    # This keeps your RAM usage near zero even with a 2GB file.
    needed_cols = ['events', 'home_team', 'away_team', 'inning_topbot']
    df = pd.read_parquet(parquet_path, columns=needed_cols)

    # The logic remains the same: identify when the team is at bat
    home_offense = df[(df['home_team'] == team_abbr) & (df['inning_topbot'] == 'Bot')]
    away_offense = df[(df['away_team'] == team_abbr) & (df['inning_topbot'] == 'Top')]
    
    team_pa = pd.concat([home_offense, away_offense]).dropna(subset=['events'])

    total_pa = len(team_pa)
    if total_pa == 0:
        print(f"No data found for {team_abbr}.")
        return None

    # Tally up the outcomes
    counts = team_pa['events'].value_counts()

    # Create the probability vector
    v = {
        '1B': counts.get('single', 0) / total_pa,
        '2B': counts.get('double', 0) / total_pa,
        '3B': counts.get('triple', 0) / total_pa,
        'HR': counts.get('home_run', 0) / total_pa,
        'WALK': (counts.get('walk', 0) + counts.get('hit_by_pitch', 0)) / total_pa,
    }
    
    v['OUT'] = 1 - sum(v.values())
    
    print(f"Calculated {total_pa} PAs for {team_abbr}. (Out rate: {v['OUT']:.1%})")
    return v

def get_team_vector_from_csv(team_abbr, csv_path=None):
    # If no path is given, just grab the most recent file we downloaded
    if csv_path is None:
        list_of_files = glob.glob('data/*.csv')
        if not list_of_files:
            raise FileNotFoundError("Data folder is empty. Run the fetcher first.")
        csv_path = max(list_of_files, key=os.path.getctime)
    
    print(f"Processing {team_abbr} from {csv_path}...")
    df = pd.read_csv(csv_path)

    # Filter for when the team is actually at bat (Home/Bottom or Away/Top)
    home_offense = df[(df['home_team'] == team_abbr) & (df['inning_topbot'] == 'Bot')]
    away_offense = df[(df['away_team'] == team_abbr) & (df['inning_topbot'] == 'Top')]
    team_pa = pd.concat([home_offense, away_offense]).dropna(subset=['events'])

    total_pa = len(team_pa)
    if total_pa == 0:
        print(f"No at-bats found for {team_abbr}.")
        return None

    # Get the raw counts of every single, double, etc.
    counts = team_pa['events'].value_counts()

    # Turn counts into percentages for the Markov Chain
    v = {
        '1B': counts.get('single', 0) / total_pa,
        '2B': counts.get('double', 0) / total_pa,
        '3B': counts.get('triple', 0) / total_pa,
        'HR': counts.get('home_run', 0) / total_pa,
        'WALK': (counts.get('walk', 0) + counts.get('hit_by_pitch', 0)) / total_pa,
    }
    
    # Everything that isn't a hit or walk is an out
    v['OUT'] = 1 - sum(v.values())
    
    print(f"Calculated {total_pa} PAs for {team_abbr}. (Out rate: {v['OUT']:.1%})")
    return v

