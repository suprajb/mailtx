import sqlite3
import os

DB_PATH = "mailtx.db"

def get_db_connection(db_path=DB_PATH):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn

def init_db(db_path=DB_PATH):
    """
    Initialize the database with the required schema.
    """
    conn = get_db_connection(db_path)
    c = conn.cursor()

    # emails table
    c.execute('''
        CREATE TABLE IF NOT EXISTS emails (
            id TEXT PRIMARY KEY,
            date TEXT,
            from_addr TEXT,
            subject TEXT,
            body_text TEXT,
            raw_path TEXT,
            content_hash TEXT UNIQUE
        )
    ''')

    #  embeddings table
    c.execute('''
        CREATE TABLE IF NOT EXISTS embeddings (
            email_id TEXT UNIQUE,
            vector BLOB,
            FOREIGN KEY(email_id) REFERENCES emails(id)
        )
    ''')

    #  tx table
    c.execute('''
        CREATE TABLE IF NOT EXISTS tx (
            id TEXT PRIMARY KEY,
            email_id TEXT,
            merchant TEXT,
            amount_cents INTEGER,
            currency TEXT,
            tx_date TEXT,
            category TEXT,
            confidence FLOAT,
            UNIQUE(email_id, amount_cents),
            FOREIGN KEY(email_id) REFERENCES emails(id)
        )
    ''')

    # FTS5 on emails if possible
    try:
        # virtual table for FTS that indexes the emails table content
        # We use the 'content' option to point to the existing emails table
        c.execute('''
            CREATE VIRTUAL TABLE IF NOT EXISTS emails_fts USING fts5(
                subject, 
                body_text, 
                content='emails', 
                content_rowid='rowid'
            )
        ''')

        # Triggers to keep FTS index up to date with the emails table
        c.execute('''
            CREATE TRIGGER IF NOT EXISTS emails_ai AFTER INSERT ON emails BEGIN
              INSERT INTO emails_fts(rowid, subject, body_text) VALUES (new.rowid, new.subject, new.body_text);
            END;
        ''')
        c.execute('''
            CREATE TRIGGER IF NOT EXISTS emails_ad AFTER DELETE ON emails BEGIN
              INSERT INTO emails_fts(emails_fts, rowid, subject, body_text) VALUES('delete', old.rowid, old.subject, old.body_text);
            END;
        ''')
        c.execute('''
            CREATE TRIGGER IF NOT EXISTS emails_au AFTER UPDATE ON emails BEGIN
              INSERT INTO emails_fts(emails_fts, rowid, subject, body_text) VALUES('delete', old.rowid, old.subject, old.body_text);
              INSERT INTO emails_fts(rowid, subject, body_text) VALUES (new.rowid, new.subject, new.body_text);
            END;
        ''')
        
    except sqlite3.OperationalError:
        # FTS5 might not be available in the sqlite3 build
        print("Note: FTS5 not available or setup failed. Continuing with standard tables.")

    conn.commit()
    conn.close()
    print(f"Database initialized at {db_path}")

