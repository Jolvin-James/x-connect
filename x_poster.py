import tweepy
import time
import os
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from dotenv import load_dotenv
from datetime import datetime

# Load Environment Variables (API Keys)
load_dotenv()

# --- Configuration ---
SHEET_NAME = "TwitterBot Content" # Exact name of your Google Sheet
JSON_KEYFILE = "credentials.json" # The file you downloaded from Google
POSTS_PER_DAY = 15

# --- X (Twitter) Authentication ---
def get_twitter_conn_v2():
    return tweepy.Client(
        consumer_key=os.getenv("X_API_KEY"),
        consumer_secret=os.getenv("X_API_SECRET"),
        access_token=os.getenv("X_ACCESS_TOKEN"),
        access_token_secret=os.getenv("X_ACCESS_TOKEN_SECRET")
    )

# --- Google Sheets Connection ---
def get_google_sheet_client():
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]
    creds = ServiceAccountCredentials.from_json_keyfile_name(JSON_KEYFILE, scope)
    client = gspread.authorize(creds)
    return client

def get_content_and_update():
    """
    Connects to G-Sheets, finds first 'Pending' row, returns text, 
    and updates status to 'Done'.
    """
    try:
        gc = get_google_sheet_client()
        
        # Open the sheet
        sh = gc.open(SHEET_NAME)
        worksheet = sh.sheet1 # First tab
        
        # Get all records as a list of dictionaries
        records = worksheet.get_all_records()
        
        # Find the first pending tweet
        row_index_to_update = None
        tweet_text = None
        
        # Iterate through records (starts at index 0 in list, but row 2 in sheet)
        for i, row in enumerate(records):
            if row['Status'] == 'Pending':
                tweet_text = row['Content']
                # i is the list index. 
                # Sheet rows start at 1. Header is row 1. Data starts row 2.
                # So the physical row number is i + 2
                row_index_to_update = i + 2 
                break
        
        if tweet_text and row_index_to_update:
            # Update the Status cell to "Done"
            # Assuming 'Status' is the 2nd column (Column B)
            worksheet.update_cell(row_index_to_update, 2, "Done")
            return tweet_text
        else:
            print("No 'Pending' tweets found in Google Sheet.")
            return None

    except Exception as e:
        print(f"Google Sheet Error: {e}")
        return None

def run_scheduler():
    client = get_twitter_conn_v2()
    
    interval_seconds = 86400 / POSTS_PER_DAY 
    
    print(f"--- X Google Sheet Bot Started ---")
    print(f"Connected to Sheet: {SHEET_NAME}")
    
    while True:
        try:
            # 1. Get Content
            tweet_text = get_content_and_update()
            
            if tweet_text is None:
                print("No content found. Checking again in 1 hour...")
                time.sleep(3600)
                continue

            # 2. Post Tweet
            print(f"Posting: {tweet_text[:30]}...")
            response = client.create_tweet(text=tweet_text)
            print(f"[{datetime.now()}] SUCCESS! ID: {response.data['id']}")
            
            # 3. Wait
            time.sleep(interval_seconds)
            
        except tweepy.errors.TooManyRequests:
            print("Rate limit hit. Sleeping 15 mins.")
            time.sleep(900)
        except Exception as e:
            print(f"Error: {e}")
            time.sleep(60)

if __name__ == "__main__":
    run_scheduler()