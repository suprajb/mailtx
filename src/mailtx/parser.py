import os
import json
import base64
import hashlib
import sqlite3
import email
from email import policy
from bs4 import BeautifulSoup
from dateutil import parser as date_parser
from .db import get_db_connection

RAW_DATA_DIR = "data/raw"

def parse_header(headers, name):
    """Extracts a specific header value by name."""
    for header in headers:
        if header['name'].lower() == name.lower():
            return header['value']
    return None

def decode_body(data):
    """Decodes base64url encoded string."""
    if not data:
        return ""
    # Add padding if needed
    padding = len(data) % 4
    if padding:
        data += '=' * (4 - padding)
    return base64.urlsafe_b64decode(data).decode('utf-8', errors='replace')

def extract_text_from_html(html_content):
    """Extracts clean text from HTML."""
    soup = BeautifulSoup(html_content, 'html.parser')
    return soup.get_text(separator='\n', strip=True)

def get_email_body(payload):
    """
    Recursively searches for the best email body.
    Prefers text/plain, falls back to text/html.
    """
    # ...existing code...
    if 'parts' in payload:
        for part in payload['parts']:
            mime_type = part.get('mimeType')
            
            if mime_type == 'text/plain':
                data = part.get('body', {}).get('data')
                if data:
                    return decode_body(data)
            
            elif mime_type == 'text/html':
                data = part.get('body', {}).get('data')
                if data:
                    html_content = decode_body(data)
                    return extract_text_from_html(html_content)
            
            # Handle nested multipart
            elif mime_type.startswith('multipart/'):
                return get_email_body(part)
                
    # If it's not multipart or just a single body
    else:
        data = payload.get('body', {}).get('data')
        mime_type = payload.get('mimeType')
        
        if data:
            decoded = decode_body(data)
            if mime_type == 'text/html':
                return extract_text_from_html(decoded)
            return decoded

    return ""

def process_raw_files(folder_path=RAW_DATA_DIR):
    """
    Iterates through raw JSON files, parses them, and inserts into SQLite.
    """
    if not os.path.exists(folder_path):
        print(f"Directory {folder_path} does not exist.")
        return

    conn = get_db_connection()
    c = conn.cursor()
    
    files = [f for f in os.listdir(folder_path) if f.endswith('.json')]
    print(f"Found {len(files)} files to process.")
    
    new_count = 0
    skip_count = 0
    error_count = 0

    for filename in files:
        file_path = os.path.join(folder_path, filename)
        
        try:
            with open(file_path, 'r') as f:
                email_data = json.load(f)
            
            email_id = email_data.get('id')
            
            # Check if we have the 'raw' format (from ingest.py)
            if 'raw' in email_data:
                try:
                    raw_base64 = email_data['raw']
                    # Decode base64url to bytes
                    padding = len(raw_base64) % 4
                    if padding:
                        raw_base64 += '=' * (4 - padding)
                    raw_bytes = base64.urlsafe_b64decode(raw_base64)
                    
                    # Parse MIME message
                    msg = email.message_from_bytes(raw_bytes, policy=policy.default)
                    
                    subject = msg['subject'] or "(No Subject)"
                    from_addr = msg['from'] or "(Unknown)"
                    date_str = msg['date']
                    
                    # Extract body using email library's logic
                    body_text = ""
                    body_part = msg.get_body(preferencelist=('plain', 'html'))
                    
                    if body_part:
                        try:
                            content = body_part.get_content()
                            if body_part.get_content_type() == 'text/html':
                                body_text = extract_text_from_html(content)
                            else:
                                body_text = content
                        except Exception:
                            # Fallback if get_content fails (e.g. encoding issues)
                            body_text = str(body_part.get_payload(decode=True), errors='replace')
                            
                except Exception as e:
                    print(f"Error parsing MIME for {filename}: {e}")
                    error_count += 1
                    continue
                    
            else:
                # Fallback to 'payload' structure (if available)
                payload = email_data.get('payload', {})
                headers = payload.get('headers', [])
                
                subject = parse_header(headers, 'Subject') or "(No Subject)"
                from_addr = parse_header(headers, 'From') or "(Unknown)"
                date_str = parse_header(headers, 'Date')
                
                # Extract Body
                body_text = get_email_body(payload)
                
                if not body_text:
                    # Fallback: sometimes body is directly in payload['body']['data']
                    data = payload.get('body', {}).get('data')
                    if data:
                        body_text = decode_body(data)

            # Parse Date
            try:
                if date_str:
                    dt = date_parser.parse(date_str)
                    iso_date = dt.strftime('%Y-%m-%d')
                else:
                    iso_date = None
            except Exception:
                iso_date = None

            # Generate Content Hash
            content_hash = hashlib.sha256(body_text.encode('utf-8')).hexdigest()
            
            # Insert into DB
            try:
                c.execute('''
                    INSERT INTO emails (id, date, from_addr, subject, body_text, raw_path, content_hash)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                ''', (email_id, iso_date, from_addr, subject, body_text, file_path, content_hash))
                new_count += 1
            except sqlite3.IntegrityError:
                # Likely duplicate content_hash or id
                skip_count += 1
                
        except Exception as e:
            print(f"Error processing {filename}: {e}")
            error_count += 1

    conn.commit()
    conn.close()
    
    print(f"Processing complete.")
    print(f"Imported: {new_count}")
    print(f"Skipped (Duplicate): {skip_count}")
    print(f"Errors: {error_count}")

def verify_recent_emails():
    """
    Prints the 5 most recent emails from the database for manual verification.
    """
    conn = get_db_connection()
    c = conn.cursor()
    
    print("\n--- Recent 5 Emails ---")
    try:
        rows = c.execute('''
            SELECT date, from_addr, subject 
            FROM emails 
            ORDER BY date DESC 
            LIMIT 5
        ''').fetchall()
        
        if not rows:
            print("No emails found in database.")
        
        for row in rows:
            print(f"[{row['date']}] {row['from_addr']}: {row['subject']}")
            
    except Exception as e:
        print(f"Error verifying emails: {e}")
    finally:
        conn.close()

