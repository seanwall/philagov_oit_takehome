import os
import requests
import sqlite3
import sqlite_utils

PHL_API_URL = "https://phl.carto.com/api/v2"

PUBLIC_CASES_TABLE = "public_cases_fc"
VIOLATIONS_TABLE = "violations"

def get(url):
    resp = requests.get(url)
    # TODO status code handling
    # TODO pagination?
    print(resp.status_code)
    return resp.json()

def query_phl(db, table, query):
    # Queries Phila Gov carto api and writes the output into the given table in the provided sqlite db.
    url = f"{PHL_API_URL}/sql?q={query}"
    json_resp = get(url)
    
    # sqlite_utils expects just a list of rows
    rows = json_resp["rows"]
    db[table].insert_all(rows)
    return True



if __name__ == "__main__":
    # Create a sqlite DB to hold all our fetched data
    db_filename = "philagov.db"

    if os.path.isfile(db_filename):
        os.remove(db_filename)
        print("removed db file")

    db = sqlite_utils.Database("philagov.db")
    # Query 311 tickets and insert into 'public_cases_fc' table in our SQLite DB
    # Using the clause 'agency_responsible LIKE License%' in lieu of 'agency_responsible='License & Inspections' since the later seemed to 
    # cause problems with how the carto API parses SQL queries passed as query params in the URL.
    query_phl(db, "public_cases_fc", "SELECT service_request_id, address, agency_responsible, requested_datetime FROM public_cases_fc WHERE requested_datetime>='2025-01-01' AND requested_datetime<'2026-01-01' AND agency_responsible LIKE 'License%';")
    # Query Licenses & Inspections violations
    # Fetch Address -> opa_account_num from AIS API