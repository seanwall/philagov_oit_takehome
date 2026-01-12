import os
import requests
import sqlite3
import sqlite_utils

PHL_CARTO_URL = "https://phl.carto.com/api/v2"
PHL_API_URL = "https://api.phila.gov/"

PUBLIC_CASES_TABLE = "public_cases_fc"
VIOLATIONS_TABLE = "violations"

def get(url):
    resp = requests.get(url)
    # TODO status code handling
    # TODO pagination?
    if resp.status_code != 200:
        print(resp.json())
    return resp.json()

def query_phl_carto(db, table, query):
    # Queries Phila Gov carto api and writes the output into the specified table in the provided sqlite db.
    url = f"{PHL_CARTO_URL}/sql?q={query}"
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
    # NB: Using the clause 'agency_responsible LIKE License%' in lieu of 'agency_responsible='License & Inspections' since the later seemed to 
    # cause problems with how the carto API parses SQL queries passed as query params in the URL. This is not ideal, but there aren't any agencies
    # other than License & Inspections that match, so I'm fine with it for the sake of this takehome.
    query_phl_carto(db, "public_cases_fc", "SELECT service_request_id, address, agency_responsible, requested_datetime FROM public_cases_fc WHERE requested_datetime>='2025-01-01' AND requested_datetime<'2026-01-01' AND agency_responsible LIKE 'License%';")
    # Query Licenses & Inspections violations
    query_phl_carto(db, "violations", "SELECT casenumber, opa_account_num, address, casestatus, violationdate, casecreateddate, casecompleteddate FROM violations WHERE casecreateddate>='2025-01-01' AND casecreateddate<'2026-01-01'")
    # Fetch Address -> opa_account_num from AIS API
    # print(get(f"{PHL_API_URL}/ais/v2/search/"))