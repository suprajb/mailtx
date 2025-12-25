import sqlite3
from .db import get_db_connection
from . import extractor

def build_ledger(process_all=False):
    """
    Iterates through emails and populates the tx table.
    
    Args:
        process_all (bool): If True, re-processes emails even if they are already in tx table.
                            (Note: This might cause unique constraint errors if not handled, 
                             so currently we just skip existing ones).
    """
    conn = get_db_connection()
    c = conn.cursor()

    # 1. Get candidates
    # for V0, we iterate all emails that are NOT in the tx table yet.
    # This allows us to resume or retry.
    print("Fetching candidate emails...")
    
    # get all email IDs that are already in tx
    c.execute("SELECT email_id FROM tx")
    existing_tx_ids = {row['email_id'] for row in c.fetchall()}
    
    # get all emails
    c.execute("SELECT id, subject, body_text, date FROM emails")
    all_emails = c.fetchall()
    
    candidates = []
    keywords = ["receipt", "order", "invoice", "payment", "transaction", "total", "purchase", "amount", "charged", "paid"]
    
    for row in all_emails:
        if row['id'] in existing_tx_ids:
            continue
            
        # Keyword filtering
        text_lower = (row['subject'] or "" + " " + row['body_text'] or "").lower()
        if any(k in text_lower for k in keywords):
            candidates.append(row)
            
    print(f"Found {len(candidates)} candidate emails (keyword filtered) out of {len(all_emails)} total.")
    
    processed_count = 0
    tx_count = 0
    
    for i, row in enumerate(candidates):
        email_id = row['id']
        subject = row['subject'] or ""
        body = row['body_text'] or ""
        email_date = row['date']
        
        # simple keyword filter to skip obvious non-receipts (optional optimization)
        # for v0, we can be generous, but let's skip very short bodies
        if len(body) < 50:
            continue

        # prepare text for LLM
        # we include Subject and Date to help the LLM
        full_text = f"Date: {email_date}\nSubject: {subject}\n\n{body[:2000]}" # Truncate to avoid context limits
        
        print(f"Processing {i+1}/{len(candidates)}: {subject[:50]}...")
        
        # 2. extract data
        tx_data = extractor.extract_tx_data(full_text)
        
        if tx_data:
            # 3. insert into tx table
            try:
                c.execute('''
                    INSERT INTO tx (id, email_id, merchant, amount_cents, currency, tx_date, category, confidence)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    f"tx_{email_id}", # Simple ID generation
                    email_id,
                    tx_data['merchant'],
                    tx_data['amount_cents'],
                    tx_data['currency'],
                    tx_data['date'],
                    tx_data['category'],
                    tx_data['confidence']
                ))
                conn.commit()
                print(f"  -> Found Transaction: {tx_data['merchant']} {tx_data['amount_cents']/100} {tx_data['currency']}")
                tx_count += 1
            except sqlite3.IntegrityError as e:
                print(f"  -> Error inserting tx: {e}")
        else:
            # print("  -> No transaction found.")
            pass
            
        processed_count += 1

    conn.close()
    print(f"Ledger build complete. Processed {processed_count} emails. Added {tx_count} transactions.")

