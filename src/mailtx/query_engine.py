import sqlite3
import json
import datetime
import ollama
from .db import get_db_connection

MODEL_NAME = "llama3.2"

SYSTEM_PROMPT = """You are a SQL query parameter extractor.
Your goal is to extract search parameters from the user's natural language query about their spending.
Return ONLY a JSON object. Do not include markdown formatting.

Required Keys:
- merchant (string, optional): The name of the merchant if specified (e.g., "Amazon", "Uber"). If not specified, omit or null.
- start_date (string, YYYY-MM-DD): The start date of the period.
- end_date (string, YYYY-MM-DD): The end date of the period.
- metric (string): 'sum' (for total spending) or 'list' (to see individual transactions).

Context:
- Today's date is: {current_date}
- If the user says "last month", calculate the start and end of the previous month relative to today.
- If the user says "December", assume the most recent December relative to today.
- If no date is specified, default to the last 30 days.
"""

def parse_intent(user_query):
    """
    Parses a natural language query into structured search parameters using an LLM.
    """
    today = datetime.date.today().isoformat()
    prompt = SYSTEM_PROMPT.format(current_date=today)
    
    try:
        response = ollama.chat(
            model=MODEL_NAME,
            messages=[
                {'role': 'system', 'content': prompt},
                {'role': 'user', 'content': user_query},
            ],
            format='json',
            options={'temperature': 0}
        )
        
        content = response['message']['content']
        # Simple cleanup if model adds markdown
        if "```json" in content:
            content = content.replace("```json", "").replace("```", "")
            
        params = json.loads(content)
        return params
    except Exception as e:
        print(f"Error parsing intent: {e}")
        return None

def execute_query(params):
    """
    Constructs and executes a SQL query based on the extracted parameters.
    """
    if not params:
        return None
        
    conn = get_db_connection()
    c = conn.cursor()
    
    query_parts = ["SELECT"]
    args = []
    
    # Determine SELECT clause
    if params.get('metric') == 'sum':
        query_parts.append("SUM(amount_cents) as total, currency")
    else:
        query_parts.append("merchant, amount_cents, currency, tx_date, category")
        
    query_parts.append("FROM tx WHERE 1=1")
    
    # Add filters
    if params.get('merchant'):
        query_parts.append("AND merchant LIKE ?")
        args.append(f"%{params['merchant']}%")
        
    if params.get('start_date'):
        query_parts.append("AND tx_date >= ?")
        args.append(params['start_date'])
        
    if params.get('end_date'):
        query_parts.append("AND tx_date <= ?")
        args.append(params['end_date'])
        
    # Group by for sum
    if params.get('metric') == 'sum':
        query_parts.append("GROUP BY currency")
    else:
        query_parts.append("ORDER BY tx_date DESC")
        
    sql = " ".join(query_parts)
    
    try:
        c.execute(sql, args)
        rows = c.fetchall()
        conn.close()
        return rows
    except sqlite3.Error as e:
        print(f"Database error: {e}")
        conn.close()
        return None

def format_result(rows, params):
    """
    Formats the query result for display.
    """
    if not rows:
        return "No transactions found matching your criteria."
        
    output = []
    if params.get('metric') == 'sum':
        for row in rows:
            total = row['total'] / 100.0 if row['total'] else 0.0
            currency = row['currency'] or 'USD'
            output.append(f"Total spent: {total:.2f} {currency}")
    else:
        output.append(f"Found {len(rows)} transactions:")
        for row in rows:
            amount = row['amount_cents'] / 100.0
            merchant = row['merchant']
            date = row['tx_date']
            curr = row['currency']
            output.append(f"- {date}: {merchant} ({amount:.2f} {curr})")
            
    return "\n".join(output)

