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
    cursor = conn.cursor()
    for row in rows:
        # The row structure from PlayerCareerStats is:
        # [player_id, season_id, _, team_id, team_abbreviation, league_id,
        #  gp, gs, min, fgm, fga, fg_pct, fg3m, fg3a, fg3_pct,
        #  ftm, fta, ft_pct, oreb, dreb, reb, ast, stl, blk, tov, pf, pts]

        # We need to skip the first player_id (since we pass it separately)
        # and the underscore column (index 2)
        if len(row) != 27:
            print(
                f"⚠️ Unexpected row length {len(row)} for player {player_id}")
            continue

        # Extract the values we need, skipping the first player_id and the underscore column
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
    with open(LOG_FILE, "a") as f:
        f.write(f"[{datetime.now()}] {player_id} - {player_name}: {error}\n")

# === FUNCTION:: Log skipped players ===


def log_skipped_player(player_id, player_name):
    with open(SKIPPED_FILE, "a") as f:
        f.write(f"{player_id},{player_name}\n")

# === FUNCTION: Update progress ===


def update_progress(progress, total, avg_time):
    percent_done = (progress / total) * 100
    remaining = total - progress
    eta = avg_time * remaining
    print(f"✅ {progress}/{total} players completed | ({percent_done:.2f}%) done | ETA: {int(eta // 60)}m {int(eta % 60)}s")

# === FUNCTION: Check if player exists ===


def player_exists(conn, player_id):
    cursor = conn.cursor()
    cursor.execute(
        "SELECT 1 FROM player_stats WHERE player_id = ? LIMIT 1", (player_id,))
    return cursor.fetchone() is not None

# === FUNCTION: Main collection ===


def collect_all_stats(conn):
    all_players = players.get_players()
    total = len(all_players)
    times = []

    # Configuration for robust API handling
    # Seconds between requests (be polite to API)
    def JITTER_DELAY(): return np.random.uniform(0.6, 1.5)  # A little bit of jitter
    MAX_RETRIES = 3       # Max retry attempts per player
    INITIAL_TIMEOUT = 20  # Start with 20s timeout (instead of 10)
    BACKOFF_FACTOR = 2    # Double timeout on each retry (30s -> 60s)

    for idx, player in enumerate(all_players):
        player_id = player['id']
        player_name = player['full_name']

        if player_exists(conn, player_id):
            print(f"⏭️ Skipping {player_name} (already collected)")
            continue

        retries = 0
        success = False

        # == MAIN LOOP ==

        while retries <= MAX_RETRIES and not success:
            try:
                start = time.time()

                # Dynamic timeout - increases with each retry
                current_timeout = INITIAL_TIMEOUT * \
                    (BACKOFF_FACTOR ** retries) * np.random.uniform(0.9, 1.1)

                print(
                    f"⌛ Fetching {player_name} (Timeout: {current_timeout}s)...")

                career = PlayerCareerStats(
                    player_id=player_id,
                    timeout=current_timeout  # Apply dynamic timeout
                )
                stats_df = career.get_data_frames()[0]

                if stats_df.empty:
                    print(f"🚫 No stats found for {player_name}")
                    success = True  # Mark as handled
                    continue

                # Save successful data
                rows = stats_df.values.tolist()
                save_player_data(conn, player_id, player_name, rows)

                # Free up space in memmory
                del stats_df, rows, career

                duration = time.time() - start
                times.append(duration)

                avg_time = sum(times) / len(times) if times else 0
                update_progress(idx + 1, total, avg_time)
                success = True

            except (Timeout, ReadTimeout, ConnectionError) as e:
                retries += 1
                if retries > MAX_RETRIES:
                    log_error(player_id, player_name,
                              f"Timeout or Connection Erorr after {MAX_RETRIES} retries: {str(e)}")
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
                break  # Exit retry loop on non-timeout errors

        # Polite delay even after success
        time.sleep(JITTER_DELAY())

# === SCRIPT RUN ===


def main():
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
