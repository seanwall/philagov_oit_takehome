import os
import requests
import sqlite3
import sqlite_utils
import concurrent.futures

PHL_CARTO_URL = "https://phl.carto.com/api/v2"
PHL_API_URL = "https://api.phila.gov/"

PUBLIC_CASES_TABLE = "public_cases_fc"
VIOLATIONS_TABLE = "violations"

def get(url):
    resp = requests.get(url)
    return resp

def query_phl_carto(db, table, query):
    # Queries Phila Gov carto api and writes the output into the specified table in the provided sqlite db.
    url = f"{PHL_CARTO_URL}/sql?q={query}"
    json_resp = get(url).json()
    
    # sqlite_utils expects just a list of rows
    rows = json_resp["rows"]
    db[table].insert_all(rows)
    return True

def fetch_opa_account_nums(addresses):
    # Given list of addresses, fetches opa_account_num from AIS API using a python thread pool
    # https://docs.python.org/3/library/concurrent.futures.html

    # Task target for thread pool - sends request to Philly AIS API and grabs opa_account_num
    def get_opa_account_num(address):
        resp = get(f"{PHL_API_URL}/ais/v2/search/{address}?opa_only")
        if resp.status_code == 404:
            # Nothing found in AIS for address, just move to next address
            return False
        else:
            return resp
    

    address_to_opa_account_num_records = []
    total = len(addresses)
    ct = 0
    with concurrent.futures.ThreadPoolExecutor(max_workers=100) as executor:
        # Submit addresses to thread pool
        future_to_address = {executor.submit(get_opa_account_num, address): address for address in addresses}
        
        # Process completed tasks
        for future in concurrent.futures.as_completed(future_to_address):
            address = future_to_address[future]
            try:
                # Progress tracking
                if ct % 500 == 0:
                    print(f"opa_account_num fetch progress: {ct}/{total}")
                ct += 1

                # Process result
                result = future.result()
                if result:
                    json_resp = result.json()
                    # Grab opa_account_num and put it in a record ready to be inserted into DB
                    for feature in json_resp['features']:
                        if feature['ais_feature_type'] == "address" and feature['match_type'] == 'exact':
                            opa_account_num = feature['properties']['opa_account_num']
                            address_to_opa_account_num_records += [{"address": address, "opa_account_num": opa_account_num}]
            except Exception as e:
                print(f"Exception in threadpool worker: {e}")
    
    return address_to_opa_account_num_records

if __name__ == "__main__":
    # Create a sqlite DB to hold all our fetched data
    db_filename = "philagov.db"
    # if os.path.isfile(db_filename):
    #     os.remove(db_filename)
    #     print("removed db file")
    db = sqlite_utils.Database(db_filename)

    # Query 311 tickets and insert into 'public_cases_fc' table in our SQLite DB
    # NB: Using the clause 'agency_responsible LIKE License%' in lieu of 'agency_responsible='License & Inspections' since the later seemed to 
    #     cause problems with how the carto API parses SQL queries passed as query params in the URL. This is not ideal, but there aren't any agencies
    #     other than License & Inspections that match, so I'm fine with it for the sake of this takehome.
    print("Fetching 311 Tickets...")
    #query_phl_carto(db, "public_cases_fc", "SELECT service_request_id, address, agency_responsible, status, requested_datetime, closed_datetime FROM public_cases_fc WHERE requested_datetime>='2025-01-01' AND requested_datetime<'2026-01-01' AND agency_responsible LIKE 'License%' AND address IS NOT NULL AND address NOT IN ('-','.');")

    # Query Licenses & Inspections violations
    print("Fetching Violations...")
    #query_phl_carto(db, "violations", "SELECT casenumber, opa_account_num, address, casestatus, violationdate, casecreateddate, casecompleteddate FROM violations WHERE casecreateddate>='2025-01-01' AND casecreateddate<'2026-01-01'")


    # Get unique addresses from public cases dataset
    con = sqlite3.connect("philagov.db")
    cur = con.cursor()
    res = cur.execute('SELECT DISTINCT address FROM public_cases_fc;')
    addresses = [row[0] for row in res.fetchall()]
    print(f"total addresses: {len(addresses)}")
    res = cur.execute('SELECT DISTINCT address FROM address_to_opa_account_num;')
    opa_account_nums = [row[0] for row in res.fetchall()]
    print(f"current fetched: {len(opa_account_nums)}")
    res = cur.execute('SELECT DISTINCT address FROM public_cases_fc EXCEPT SELECT address FROM address_to_opa_account_num;')
    missing = [row[0] for row in res.fetchall()]
    print(f"missing: {len(missing)}")

    # Fetch Address -> opa_account_num from AIS API
    print("Fetching opa_account_nums...")
    address_opa_records = fetch_opa_account_nums(missing)
    db["address_to_opa_account_num"].insert_all(address_opa_records)