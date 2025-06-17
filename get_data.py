import os
import sys
import sqlite3
import time
from nba_api.stats.static import players
from nba_api.stats.endpoints import PlayerCareerStats
from datetime import datetime
import traceback
from requests.exceptions import (
    Timeout,
    ReadTimeout,
    ConnectionError,
    HTTPError,
    RequestException
)
import numpy as np

# === CONFIG ===

DATA_FOLDER = "./data"
DB_PATH = "./data/nba_2000-25_stats.db"
LOG_FILE = "./data/error_log.txt"
SKIPPED_FILE = "./data/skipped_players.txt"

# === FUNCTION: Pre-check setup ===

def precheck_and_setup():
    """
    Ensure data folder exists and establish SQLite DB connection.
    
    Returns:
        sqlite3.Connection: Database connection object.
        
    Exits the program if folder creation or DB connection fails.
    """
    try:
        if not os.path.exists(DATA_FOLDER):
            print(f"📁 '{DATA_FOLDER}' folder missing. Creating it...")
            os.makedirs(DATA_FOLDER)
        print("✅ Data folder check passed.")
    except Exception as e:
        print(f"❌ Failed to create '{DATA_FOLDER}' folder: {e}")
        sys.exit(1)

    try:
        conn = sqlite3.connect(DB_PATH, check_same_thread=False)
        print("✅ Database connection established.")
        return conn
    except Exception as e:
        print(f"❌ Could not connect to DB: {e}")
        sys.exit(1)

# === FUNCTION: Setup database schema ===

def setup_database(conn):
    """
    Create the player_stats table in the database if it does not exist.
    
    Args:
        conn (sqlite3.Connection): Active SQLite connection.
    """
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS player_stats (
            player_id INTEGER,
            player_name TEXT,
            season_id TEXT,
            team_id INTEGER,
            team_abbreviation TEXT,
            league_id TEXT,
            gp INTEGER,
            gs INTEGER,
            min TEXT,
            fgm INTEGER,
            fga INTEGER,
            fg_pct REAL,
            fg3m INTEGER,
            fg3a INTEGER,
            fg3_pct REAL,
            ftm INTEGER,
            fta INTEGER,
            ft_pct REAL,
            oreb INTEGER,
            dreb INTEGER,
            reb INTEGER,
            ast INTEGER,
            stl INTEGER,
            blk INTEGER,
            tov INTEGER,
            pf INTEGER,
            pts INTEGER,
            PRIMARY KEY (player_id, season_id)
        )
    ''')
    conn.commit()

# === FUNCTION: Save player data ===

def save_player_data(conn, player_id, player_name, rows):
    """
    Insert or update multiple rows of player career stats into the database.
    
    Args:
        conn (sqlite3.Connection): Active SQLite connection.
        player_id (int): NBA player ID.
        player_name (str): NBA player full name.
        rows (list): List of player stats rows from API.
    """
    cursor = conn.cursor()
    for row in rows:
        if len(row) != 27:
            print(f"⚠️ Unexpected row length {len(row)} for player {player_id}")
            continue

        season_id = row[1]
        team_id = row[3]
        team_abbreviation = row[4]
        league_id = row[5]
        gp = row[6]
        gs = row[7]
        minutes = row[8]
        fgm = row[9]
        fga = row[10]
        fg_pct = row[11]
        fg3m = row[12]
        fg3a = row[13]
        fg3_pct = row[14]
        ftm = row[15]
        fta = row[16]
        ft_pct = row[17]
        oreb = row[18]
        dreb = row[19]
        reb = row[20]
        ast = row[21]
        stl = row[22]
        blk = row[23]
        tov = row[24]
        pf = row[25]
        pts = row[26]

        cursor.execute('''
            INSERT OR REPLACE INTO player_stats (
                player_id, player_name, season_id, team_id, team_abbreviation, league_id,
                gp, gs, min, fgm, fga, fg_pct, fg3m, fg3a, fg3_pct,
                ftm, fta, ft_pct, oreb, dreb, reb, ast, stl, blk, tov, pf, pts
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            player_id, player_name, season_id, team_id, team_abbreviation, league_id,
            gp, gs, minutes, fgm, fga, fg_pct, fg3m, fg3a, fg3_pct,
            ftm, fta, ft_pct, oreb, dreb, reb, ast, stl, blk, tov, pf, pts
        ))
    conn.commit()

# === FUNCTION: Log error ===

def log_error(player_id, player_name, error):
    """
    Append error messages to a log file with timestamp.
    
    Args:
        player_id (str|int): Player ID or identifier.
        player_name (str): Player name or identifier.
        error (str): Error message or traceback string.
    """
    with open(LOG_FILE, "a") as f:
        f.write(f"[{datetime.now()}] {player_id} - {player_name}: {error}\n")

# === FUNCTION: Log skipped players ===

def log_skipped_player(player_id, player_name):
    """
    Append skipped player info to a CSV file.
    
    Args:
        player_id (int): NBA player ID.
        player_name (str): NBA player full name.
    """
    with open(SKIPPED_FILE, "a") as f:
        f.write(f"{player_id},{player_name}\n")

# === FUNCTION: Update progress ===

def update_progress(progress, total, avg_time, cooldown_time, batch_size):
    """
    Print the progress of data collection with estimated time remaining (ETA).
    
    Args:
        progress (int): Number of players processed so far.
        total (int): Total number of players.
        avg_time (float): Average time per player in seconds.
        cooldown_time (int): Cooldown duration in seconds after each batch.
        batch_size (int): Number of players processed per batch.
    """
    percent_done = (progress / total) * 100
    remaining = total - progress

    cooldowns_left = max(int(np.ceil(remaining / batch_size)), 0)
    total_cooldown_time = cooldowns_left * cooldown_time

    eta_seconds = (avg_time * remaining) + total_cooldown_time
    eta_minutes = int(eta_seconds // 60)
    eta_secs = int(eta_seconds % 60)

    print(f"✅ {progress}/{total} players completed | ({percent_done:.2f}%) done | ETA: {eta_minutes}m {eta_secs}s")

# === FUNCTION: Check if player exists ===

def player_exists(conn, player_id):
    """
    Check if a player's stats are already in the database.
    
    Args:
        conn (sqlite3.Connection): Active SQLite connection.
        player_id (int): NBA player ID.
        
    Returns:
        bool: True if player data exists, False otherwise.
    """
    cursor = conn.cursor()
    cursor.execute(
        "SELECT 1 FROM player_stats WHERE player_id = ? LIMIT 1", (player_id,))
    return cursor.fetchone() is not None

# === FUNCTION: Main collection ===

def collect_all_stats(conn):
    """
    Collect career stats for all NBA players from nba_api and save to DB.
    Handles retries, cooldowns, progress updates, and error logging.
    
    Args:
        conn (sqlite3.Connection): Active SQLite connection.
    """
    all_players = players.get_players()
    total = len(all_players)
    times = []

    # == CONFIG ==
    def JITTER_DELAY(): return np.random.uniform(0.6, 1.5)
    MAX_RETRIES = 3
    INITIAL_TIMEOUT = 20
    BACKOFF_FACTOR = 2

    BATCH_SIZE = 500
    COOLDOWN_TIME = 60  # seconds (long pause after each batch)

    for idx, player in enumerate(all_players):
        player_id = player['id']
        player_name = player['full_name']

        if player_exists(conn, player_id):
            print(f"⏭️ Skipping {player_name} (already collected)")
            continue

        retries = 0
        success = False

        while retries <= MAX_RETRIES and not success:
            try:
                start = time.time()

                current_timeout = INITIAL_TIMEOUT * \
                    (BACKOFF_FACTOR ** retries) * np.random.uniform(0.9, 1.1)
                print(
                    f"⌛ Fetching {player_name} (Timeout: {current_timeout:.1f}s)...")

                career = PlayerCareerStats(
                    player_id=player_id, timeout=current_timeout)
                stats_df = career.get_data_frames()[0]

                if stats_df.empty:
                    print(f"🚫 No stats found for {player_name}")
                    success = True
                    break

                rows = stats_df.values.tolist()
                save_player_data(conn, player_id, player_name, rows)

                del stats_df, rows, career

                duration = time.time() - start
                times.append(duration)

                avg_time = sum(times) / len(times) if times else 0
                update_progress(idx + 1, total, avg_time,
                                COOLDOWN_TIME, BATCH_SIZE)
                success = True

            except (Timeout, ReadTimeout, ConnectionError) as e:
                retries += 1
                if retries > MAX_RETRIES:
                    log_error(
                        player_id, player_name, f"Timeout or Connection Error after {MAX_RETRIES} retries: {str(e)}")
                    log_skipped_player(player_id, player_name)
                    print(
                        f"❌ Failed {player_name} after {MAX_RETRIES} retries")
                else:
                    wait_time = JITTER_DELAY() * (BACKOFF_FACTOR ** retries)
                    print(
                        f"↩️ Retry {retries}/{MAX_RETRIES} in {wait_time:.1f}s...")
                    time.sleep(wait_time)

            except HTTPError as e:
                log_error(player_id, player_name, f"HTTP error: {str(e)}")
                log_skipped_player(player_id, player_name)
                print(f"❗ HTTP error for {player_name}: {str(e)}")
                break

            except Exception as e:
                log_error(player_id, player_name, traceback.format_exc())
                log_skipped_player(player_id, player_name)
                print(f"❗ Unexpected error with {player_name}: {str(e)}")
                break

        # Polite delay after every request
        time.sleep(JITTER_DELAY())

        # Long break every N players
        if (idx + 1) % BATCH_SIZE == 0:
            print(
                f"😴 Cooldown: Pausing for {COOLDOWN_TIME} seconds after batch of {BATCH_SIZE} players...")
            for remaining in range(COOLDOWN_TIME, 0, -1):
                sys.stdout.write(f"\r⏳ Countdown: {remaining}s remaining... ")
                sys.stdout.flush()
                time.sleep(1)
            sys.stdout.write(
                "\r⌛ Countdown: Complete ✅                      \n")
            print(
                f"Restarting collecting data for the next {BATCH_SIZE} players!\n")

# === SCRIPT RUN ===

def main():
    """
    Main entry point to start the NBA data collection process.
    Sets up folders, database, and begins stats collection.
    """
    print("🚀 Starting NBA data collection...")
    conn = precheck_and_setup()
    setup_database(conn)
    collect_all_stats(conn)
    print("🏁 Finished collecting NBA data.")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        log_error(player_id="MAIN", player_name="SCRIPT_STARTUP", error=e)
        print("❌ Fatal error – see the Error Log (error_log.txt) for details.")
