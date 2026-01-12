import requests
import sqlite3
import sqlite_utils

PHL_API_URL = "https://phl.carto.com/api/v2"

PUBLIC_CASES_TABLE = "public_cases_fc"
VIOLATIONS_TABLE = "violations"

def get(url):
    resp = requests.get(url)
    print(resp.status_code)
    return resp.json()

def query_phl(query):
    # Queries Phila Gov carto api and pipes the output into a sqlite db.
    url = f"{PHL_API_URL}/sql?q={query}"
    json_resp = get(url)
    print(json_resp)

    db = sqlite_utils.Database("test.db")
    db["test"].insert_all(json_resp["rows"])
    return True



if __name__ == "__main__":
    query_phl("SELECT * FROM public_cases_fc LIMIT 1")