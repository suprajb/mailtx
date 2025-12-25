import json
import ollama

# Using llama3.2 as it is efficient and widely available. 
# You can switch to 'llama3.1' or 'qwen2.5' if preferred.
MODEL_NAME = "llama3.2" 

SYSTEM_PROMPT = """You are a data parser. Extract financial details from the email text provided.
Return ONLY a JSON object. Do not include markdown formatting like ```json ... ```.

Required Keys:
- merchant (string): The name of the vendor.
- amount (float): The total transaction amount.
- currency (string): The currency code (e.g., 'USD', 'EUR', 'INR').
- date (string): The transaction date in YYYY-MM-DD format.
- category (string): One of ['Food', 'Transport', 'Shopping', 'Subscription', 'Utilities', 'Travel', 'Other'].

If the email is NOT a receipt, invoice, or transaction confirmation, return an empty JSON object {}.
"""

def extract_tx_data(email_text):
    """
    Extracts transaction data from email text using a local LLM.
    Returns a dict with keys: merchant, amount_cents, currency, date, category.
    Returns None if extraction fails or no transaction found.
    """
    try:
        response = ollama.chat(
            model=MODEL_NAME,
            messages=[
                {'role': 'system', 'content': SYSTEM_PROMPT},
                {'role': 'user', 'content': email_text},
            ],
            format='json',
            options={'temperature': 0} # Deterministic output
        )
        
        content = response['message']['content']
        
        # Parse JSON
        try:
            data = json.loads(content)
        except json.JSONDecodeError:
            # Simple fallback to find first { and last }
            start = content.find('{')
            end = content.rfind('}') + 1
            if start != -1 and end != -1:
                data = json.loads(content[start:end])
            else:
                return None

        # check if empty (not a receipt)
        if not data or 'amount' not in data:
            return None
            
        # Post-processing
        amount = float(data.get('amount', 0.0))
        amount_cents = int(round(amount * 100))
        
        return {
            'merchant': data.get('merchant', 'Unknown'),
            'amount_cents': amount_cents,
            'currency': data.get('currency', 'USD'),
            'date': data.get('date'),
            'category': data.get('category', 'Other'),
            'confidence': 1.0 
        }
        
    except Exception as e:
        # print(f"Extraction error: {e}") # Optional: uncomment for debugging
        return None

