import os
import requests

SUPABASE_URL = os.environ["SUPABASE_URL"].rstrip("/")
SUPABASE_SERVICE_KEY = os.environ["SUPABASE_SERVICE_KEY"]
ODDS_TABLE = os.environ.get("ODDS_TABLE", "Odds")

def get_supabase_headers():
    return {
        "apikey": SUPABASE_SERVICE_KEY,
        "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}"
    }

def is_arbitrage(odds_a: float, odds_b: float) -> bool:
    """
    Returns True if an arbitrage opportunity exists
    for a two-outcome event given decimal odds.

    Args:
        odds_a (float): Decimal odds for outcome A
        odds_b (float): Decimal odds for outcome B

    Returns:
        bool: True if arbitrage exists, False otherwise
    """
    implied_prob_a = 1 / odds_a
    implied_prob_b = 1 / odds_b
    total_prob = implied_prob_a + implied_prob_b
    
    return total_prob < 1.0

def fetch_odds():
    url = f"{SUPABASE_URL}/rest/v1/{ODDS_TABLE}?select=*"
    headers = get_supabase_headers()
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    return response.json()

def lambda_handler(event, context):
    print("Fetching odds records from Supabase...")
    odds_records = fetch_odds()
    print(f"Fetched {len(odds_records)} records.")