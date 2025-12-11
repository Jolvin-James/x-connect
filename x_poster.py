import tweepy
import time
import os
import pandas as pd
from dotenv import load_dotenv
from datetime import datetime

# Load credentials
load_dotenv()

API_KEY = os.getenv("X_API_KEY")
API_SECRET = os.getenv("X_API_SECRET")
ACCESS_TOKEN = os.getenv("X_ACCESS_TOKEN")
ACCESS_SECRET = os.getenv("X_ACCESS_TOKEN_SECRET")

EXCEL_FILE = "tweets.xlsx"

def get_twitter_conn_v2():
    return tweepy.Client(
        consumer_key=API_KEY,
        consumer_secret=API_SECRET,
        access_token=ACCESS_TOKEN,
        access_token_secret=ACCESS_SECRET
    )

def get_content_to_post():
    """
    Reads the Excel file, picks the first 'Pending' tweet, 
    updates it to 'Done', saves the file, and returns the text.
    """
    try:
        # Read the Excel file
        df = pd.read_excel(EXCEL_FILE)
        
        # Ensure columns exist (Sanity check)
        if 'Status' not in df.columns or 'Content' not in df.columns:
            print("Error: Excel file must have 'Content' and 'Status' columns.")
            return None

        # Find the first row where Status is 'Pending'
        pending_tweets = df[df['Status'] == 'Pending']
        
        if pending_tweets.empty:
            print("No 'Pending' tweets found in Excel.")
            return None
        
        # Get the index of the first pending tweet
        index_to_post = pending_tweets.index[0]
        
        # Extract the content
        tweet_text = df.at[index_to_post, 'Content']
        
        # Update the status to 'Done'
        df.at[index_to_post, 'Status'] = 'Done'
        
        # Save the updated dataframe back to Excel
        # index=False ensures we don't add an extra number column every time
        df.to_excel(EXCEL_FILE, index=False)
        
        return tweet_text

    except FileNotFoundError:
        print(f"Error: Could not find {EXCEL_FILE}")
        return None
    except PermissionError:
        print(f"Error: Please close {EXCEL_FILE} before running the script.")
        return None
    except Exception as e:
        print(f"An unexpected error occurred reading Excel: {e}")
        return None

def run_scheduler():
    client = get_twitter_conn_v2()
    
    posts_per_day = 15
    interval_seconds = 86400 / posts_per_day 
    
    print(f"--- X Excel Bot Started ---")
    print(f"Reading from: {EXCEL_FILE}")
    print(f"Interval: One post every {interval_seconds/60:.2f} minutes")

    while True:
        try:
            # 1. Get Content from Excel
            tweet_text = get_content_to_post()
            
            # If function returns None, we are out of tweets or have an error
            if tweet_text is None:
                print("Stopping script: No content available or file error.")
                break 

            # 2. Post Tweet
            print(f"Attempting to post: {tweet_text[:30]}...")
            response = client.create_tweet(text=tweet_text)
            
            print(f"[{datetime.now()}] SUCCESS! Tweet Sent. ID: {response.data['id']}")
            
            # 3. Wait for next slot
            time.sleep(interval_seconds)
            
        except tweepy.errors.TooManyRequests:
            print("Rate limit hit. Waiting for 15 minutes...")
            time.sleep(900) 
        except Exception as e:
            print(f"Error occurred during posting: {e}")
            # Wait 60 seconds before retrying to avoid spamming errors
            time.sleep(60) 

if __name__ == "__main__":
    run_scheduler()