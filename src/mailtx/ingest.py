import os.path
import json
import datetime
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build

# If modifying these scopes, we should delete the file token.json.
SCOPES = ['https://www.googleapis.com/auth/gmail.readonly']
DATA_DIR = "data/raw"

def authenticate_gmail():
    """Shows basic usage of the Gmail API.
    Lists the user's Gmail labels.
    """
    creds = None
    # The file token.json stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    if os.path.exists('token.json'):
        creds = Credentials.from_authorized_user_file('token.json', SCOPES)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            if not os.path.exists('credentials.json'):
                raise FileNotFoundError("credentials.json not found. Please download it from Google Cloud Console.")
            
            flow = InstalledAppFlow.from_client_secrets_file(
                'credentials.json', SCOPES)
            creds = flow.run_local_server(port=8080)
        # save the credentials for the next run
        with open('token.json', 'w') as token:
            token.write(creds.to_json())

    return build('gmail', 'v1', credentials=creds)

def download_recent_emails(days=90):
    """Downloads raw emails from the last n days."""
    service = authenticate_gmail()
    
    # Calculate date for query
    date_after = (datetime.datetime.now() - datetime.timedelta(days=days)).strftime('%Y/%m/%d')
    query = f"after:{date_after}"
    print(f"Querying emails {query}...")

    # Ensure data directory exists
    os.makedirs(DATA_DIR, exist_ok=True)

    messages = []
    next_page_token = None
    
    while True:
        results = service.users().messages().list(userId='me', q=query, pageToken=next_page_token).execute()
        batch_messages = results.get('messages', [])
        messages.extend(batch_messages)
        
        print(f"Found {len(batch_messages)} messages in this batch...")
        
        next_page_token = results.get('nextPageToken')
        if not next_page_token:
            break

    print(f"Total messages found: {len(messages)}")

    for i, msg in enumerate(messages):
        msg_id = msg['id']
        file_path = os.path.join(DATA_DIR, f"{msg_id}.json")
        
        if os.path.exists(file_path):
            print(f"[{i+1}/{len(messages)}] Skipping {msg_id} (already exists)")
            continue

        try:
            # We fetch 'raw' format to preserve the original email content perfectly for later parsing
            # The response will be a JSON object containing a 'raw' field (base64url encoded)
            # TODO: Consider using 'full' format if metadata is needed
            message_full = service.users().messages().get(userId='me', id=msg_id, format='raw').execute()
            
            with open(file_path, 'w') as f:
                json.dump(message_full, f)
            
            print(f"[{i+1}/{len(messages)}] Downloaded {msg_id}")
        except Exception as e:
            print(f"Error downloading {msg_id}: {e}")

