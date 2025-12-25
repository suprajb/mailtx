import sqlite3
import json
import numpy as np
import ollama
from .db import get_db_connection

MODEL_NAME = "nomic-embed-text"

def generate_embeddings():
    """
    Generates embeddings for emails that don't have them yet.
    """
    conn = get_db_connection()
    c = conn.cursor()

    # fetch emails that are not in the embeddings table
    # We use a LEFT JOIN or NOT IN clause
    c.execute('''
        SELECT e.id, e.subject, e.body_text 
        FROM emails e
        LEFT JOIN embeddings emb ON e.id = emb.email_id
        WHERE emb.email_id IS NULL
    ''')
    
    emails_to_process = c.fetchall()
    total = len(emails_to_process)
    print(f"Found {total} emails to embed.")

    for i, row in enumerate(emails_to_process):
        email_id = row['id']
        subject = row['subject'] or ""
        body = row['body_text'] or ""
        
        # truncate body to 512 chars to fit context window and save time
        # TODO: handle longer bodies with chunking if needed
        truncated_body = body[:512]
        
        input_text = f"Subject: {subject}\nBody: {truncated_body}"
        
        try:
            response = ollama.embeddings(model=MODEL_NAME, prompt=input_text)
            vector = response['embedding']
            
            # serialize vector to JSON bytes
            vector_blob = json.dumps(vector).encode('utf-8')
            
            c.execute('''
                INSERT INTO embeddings (email_id, vector)
                VALUES (?, ?)
            ''', (email_id, vector_blob))
            
            if (i + 1) % 10 == 0:
                print(f"Processed {i + 1}/{total}...")
                conn.commit()
                
        except Exception as e:
            print(f"Error embedding email {email_id}: {e}")

    conn.commit()
    conn.close()
    print("Embedding generation complete.")

def find_similar(query_text, top_k=10):
    """
    Finds emails semantically similar to the query text.
    """
    try:
        query_response = ollama.embeddings(model=MODEL_NAME, prompt=query_text)
        query_vector = np.array(query_response['embedding'])
    except Exception as e:
        print(f"Error generating query embedding: {e}")
        return []

    conn = get_db_connection()
    c = conn.cursor()
    
    c.execute('SELECT email_id, vector FROM embeddings')
    rows = c.fetchall()
    
    similarities = []
    
    for row in rows:
        email_id = row['email_id']
        vector_blob = row['vector']
        
        try:
            # deserialize vector
            vector = np.array(json.loads(vector_blob.decode('utf-8')))
            
            # calculate Cosine Similarity
            # sim = (A . B) / (||A|| * ||B||)
            dot_product = np.dot(query_vector, vector)
            norm_a = np.linalg.norm(query_vector)
            norm_b = np.linalg.norm(vector)
            
            if norm_a == 0 or norm_b == 0:
                similarity = 0
            else:
                similarity = dot_product / (norm_a * norm_b)
                
            similarities.append((email_id, similarity))
            
        except Exception as e:
            print(f"Error processing vector for {email_id}: {e}")
            continue
            
    conn.close()
    
    # sort by similarity descending
    similarities.sort(key=lambda x: x[1], reverse=True)
    
    return similarities[:top_k]

