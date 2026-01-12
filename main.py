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
    # Given list of addresses, fetches opa_account_num from AIS API
    num_addresses = len(addresses)
    address_opa_records = []
    ct = 0
    for address in addresses:
        ct += 1
        resp = get(f"{PHL_API_URL}/ais/v2/search/{address}")
        if resp.status_code == 404:
            # Nothing found in AIS for address, just move to next address
            continue
        json_resp = resp.json()
        for feature in json_resp['features']:
            if feature['ais_feature_type'] == "address" and feature['match_type'] == 'exact':
                properties = feature['properties']
                ais_address = properties['street_address']
                opa_account_num = properties['opa_account_num']
                if address != ais_address:
                    print(f"Something weird; 311 Address: {address}, AIS address: {ais_address}, OPA num: {opa_account_num}")
                address_opa_records += [{"address": address, "opa_account_num": opa_account_num}]
                if ct % 100 == 0:
                    print(f"Found {ct} addresses out of {num_addresses}")

    return address_opa_records

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
   #query_phl_carto(db, "public_cases_fc", "SELECT service_request_id, address, agency_responsible, requested_datetime FROM public_cases_fc WHERE requested_datetime>='2026-01-01' AND requested_datetime<'2027-01-01' AND agency_responsible LIKE 'License%' AND address IS NOT NULL AND address NOT IN ('-','.');")
    
    # Query Licenses & Inspections violations
    # NB: I noticed there were rows with duplicate casenumbers in the violations dataset, which I'm not sure how to interpret. 
    #     Grouping this query by (casenumber, opa_account_num, address, casestatus) to eliminate the duplicates, and
    #     taking the min of the datetime fields - this likely results in fudging those fields across rows but accuracy of those fields isn't relevant for the required analysis.
    #query_phl_carto(db, "violations", "SELECT casenumber, opa_account_num, address, casestatus, min(violationdate) as violationdate, min(casecreateddate) as casecreateddate, min(casecompleteddate) as casecompleteddate FROM violations WHERE casecreateddate>='2026-01-01' AND casecreateddate<'2027-01-01' GROUP BY casenumber, opa_account_num, address, casestatus")
    
    # Fetch Address -> opa_account_num from AIS API

    # Get unique addresses from public cases dataset
    con = sqlite3.connect("philagov.db")
    cur = con.cursor()
    res = cur.execute('SELECT DISTINCT address FROM public_cases_fc;')
    addresses = [row[0] for row in res.fetchall()]
    a2 = addresses[0:10]
    address_opa_records = fetch_opa_account_nums(a2)
    
    db["address_to_opa_account_num"].insert_all(address_opa_records)

