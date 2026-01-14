# PhilaGov OIT Takehome
Sean Wallace

## Running Locally
The script can be run on the OIT server with `python3 main.py`. You will first need to `pip install -r requirements.txt` (though it's just one package).

Note that I've submitted this repo with the code targetting **2026** data, as the runtime is much smaller, but the analysis section below is for 2025. I've included the sqlite DB `philagov2025.db` which includes all of the data I retrieved and used for analysis for 2025.

## Data Retrieval
My data retrieval approach was largely the same for all the data sources: fetch the data and store it locally in a sqlite database. For the analysis I wanted to be able to work with SQL and having a local copy of any data that I fetched made for easy iteration. I used the package `sqlite_utils` for creating the sqlite db - as it had easy functionality for inserting json data into a sqlite database.

Getting both the 311 tickets and the Violations dataset was fairly straightforward, as they are accessible through a carto API, which allows the use of SQL. Requests to the following two URLs were sufficient:

**311 Tickets**

```https://phl.carto.com/api/v2/sql?q=SELECT service_request_id, address, agency_responsible, status, requested_datetime, closed_datetime FROM public_cases_fc WHERE requested_datetime>='2025-01-01' AND requested_datetime<'2026-01-01' AND agency_responsible LIKE 'License%' AND address IS NOT NULL AND address NOT IN ('-','.');```

**Violations**

```https://phl.carto.com/api/v2/sql?q=SELECT casenumber, opa_account_num, address, casestatus, violationdate, casecreateddate, casecompleteddate FROM violations WHERE casecreateddate>='2025-01-01' AND casecreateddate<'2026-01-01'```

Both queries only return records from 2025 - I suppose it's possible that a 311 ticket in 2025 could have a violation in 2026, but I figured this wouldn't introduce much error into the overall analysis if those violations were missed.

Additionally, for 311 Tickets, I filtered out NULL or empty addresses. This wasn't explicitly necessary (it could have been handled locally afterwards), but it seemed nice to trim the request as much as possible. This was also why I chose to include the `License & Inspections` clause in the carto query. I hit a little snag there as it seemed like carto's url query parsing failed when I tried to use the clause `agency_responsible = 'License & Inspection'` - it seemed like the spaces in `License & Inspection` tripped something up. Since there's no other entries in the `agency_responsible` field that started with `License`, I could use a `LIKE License%` to get around this. It's a bit hacky and not a great long term solution but I was fine with that for now.

**opa_account_num**

Fetching the opa_account_num from the AIS API was a little less straightforward. The API is easy enough to use, but I needed to fetch ~30,000 addresses. In order to speed this up I sent out these requests in a threadpool (see `fetch_opa_account_nums`) so that they could be executed asynchronously. 

This was a significant speedup, but still took about 20 minutes. Unfortunately at the tail end of this it seems like the AIS API went down. I assume this was my fault, and I apologize. I had added another table to my sqlite DB to hold the `address:opa_account_num` map, so I found the addresses that I still needed the `opa_account_num` for by getting all the addresses from the 311 tickets that didnt show up in that `opa_account_num` table, and doing another much smaller batch of requests to the AIS api (I was only missing a couple thousand addresses).

I'm not too happy with what I ended up with here - it's tough to balance not overloading the AIS API with having this execute in a reasonable amount of time.

## Data Assumptions
The biggest challenge with this data is the fact that there can be violations for an address that don't stem from a 311 ticket - this is difficult if we're joining based on `opa_account_num`. I tried to look at the case created time on the violations for a given address and see if I could use the updated time in the 311 ticket to explicitly tie a 311 ticket to a violation - just by looking at some of the records it seemed like 311 tickets might get updated around the time the violation is issued. This was a very tunous connection - I decided to just assume that any violation that had an address fromm the 311 ticket dataset stemmed from that 311 ticket. 

There were a couple other points I was thinking about as well:
- There appear to be multiple violations in the Violations dataset for the same case number, which I assume to mean that one 311 Ticket could cause more than one violation
- There could be multiple 311 tickets reporting the same issue (and therefor, be related to the same violations)

I decided that *any* (311 ticket, violation) pair with matching addresses (by way of opa_account_num) would be treated as that violation being a result of that 311 ticket. This is a pretty gross generalization, but I couldn't come up with a robust solution here. You could probably reduce error by comparing timestamps or something - a 311 ticket obviously didnt result in a violation if the violation was created before the 311 ticket - but that was more work than I thought it was worth in this case.

## Analysis

### How many service requests since the beginning of the year has 311 associated with “License & Inspections” as the agency_responsible?** = `44,748`

Since I did this filter in the carto query, this is just `SELECT COUNT(*) FROM public_cases_fc;`: `44748`.

### What percentage of these service requests have resulted in the issuance of a code violation?** = `55.46%`

For this, I want the number of 311 Tickets which have an address with an `opa_account_number` that shows up in `violations`. To get this I join the 311 tickets to my opa_account_num dataset, and get a COUNT of all rows that have an opa_account_num in `violations`.

`SELECT COUNT(*) FROM public_cases_fc pc JOIN address_to_opa_account_num oan ON pc.address=oan.address WHERE oan.opa_account_num IN (SELECT DISTINCT opa_account_num FROM violations);`= `24,817`
Percent of all 311 tickets = `55.46%`

### What percentage of these service requests have not been closed? (i.e. L&I has not finished inspecting them)* = `63.11%`
For this, I want the number of 311 Tickets which have an address with an `opa_account_number` that shows up in `violations` AND shows up in a row in `violations` where `case_status` is not `CLOSED`.

`SELECT COUNT(*) FROM public_cases_fc pc JOIN address_to_opa_account_num oan ON pc.address=oan.address WHERE oan.opa_account_num IN (SELECT DISTINCT opa_account_num FROM violations WHERE casestatus!='CLOSED');` = `15,663`

I interpreted the question as being what percent *of the tickets that have a violation* have an open violation, so (num tickets with open violation)/(num tickets with violaion) = `63.11%`

## Epilogue

Sorry for the long winded README! I'll note that I have not run my script to completion from start to finish for 2025. The wait time for the AIS API requests is long and would make for a hefty cycle time. The analysis above is for the 2025 dataset, but I've uploaded the repo looking at tickets and violations so far in 2026, which is a much more reasonable runtime and 

