import os
import time
import hashlib
import requests
import psycopg2
from datetime import date
from dotenv import load_dotenv
from tqdm import tqdm

# ------------------ CONFIG ------------------
BATCH_SIZE = 100
SLEEP_SECONDS = 2
TIMEOUT = 20
HEADERS = {"User-Agent": "ISSN-Metadata-Pipeline/1.0"}

# ------------------ LOAD ENV ------------------
load_dotenv()

DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT"),
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
}

# ------------------ DB CONNECTION ------------------
def get_db():
    return psycopg2.connect(**DB_CONFIG)

# ------------------ UTILS ------------------
def chunk_list(lst, size):
    for i in range(0, len(lst), size):
        yield lst[i:i + size]

def generate_hash(row: dict) -> str:
    raw = "|".join([str(v) for v in row.values()])
    return hashlib.sha256(raw.encode()).hexdigest()

# ------------------ API CALLS ------------------
def fetch_crossref(issn):
    try:
        r = requests.get(
            f"https://api.crossref.org/journals/{issn}",
            headers=HEADERS,
            timeout=TIMEOUT
        )
        if r.status_code == 200:
            return r.json()["message"]
    except Exception:
        pass
    return None

def fetch_openalex(issn):
    try:
        r = requests.get(
            "https://api.openalex.org/sources",
            params={"filter": f"issn:{issn}"},
            headers=HEADERS,
            timeout=TIMEOUT
        )
        if r.status_code == 200 and r.json()["results"]:
            return r.json()["results"][0]
    except Exception:
        pass
    return None

# ------------------ LOAD ISSNS ------------------
def load_issns():
    with get_db() as conn, conn.cursor() as cur:
        cur.execute("SELECT issn FROM issn_master")
        return [r[0] for r in cur.fetchall()]

# ------------------ CHANGE DETECTION ------------------
def record_exists(cur, issn, record_hash):
    cur.execute("""
        SELECT 1 FROM issn_metadata_fact
        WHERE issn = %s AND record_hash = %s
        LIMIT 1
    """, (issn, record_hash))
    return cur.fetchone() is not None

# ------------------ MAIN PIPELINE ------------------
def run_pipeline():
    issns = load_issns()
    today = date.today()

    print(f"Total ISSNs: {len(issns)}")

    with get_db() as conn:
        cur = conn.cursor()

        for batch in tqdm(list(chunk_list(issns, BATCH_SIZE))):
            rows_to_insert = []

            for issn in batch:
                crossref = fetch_crossref(issn)
                openalex = fetch_openalex(issn)

                row = {
                    "issn": issn,
                    "journal_title": crossref.get("title") if crossref else None,
                    "publisher": crossref.get("publisher") if crossref else None,
                    "subjects": ", ".join(crossref.get("subjects", [])) if crossref else None,
                    "country": openalex.get("country_code") if openalex else None,
                    "open_access": openalex.get("is_oa") if openalex else None,
                    "doi_prefix": crossref.get("prefix") if crossref else None,
                    "source": "crossref+openalex",
                    "fetch_date": today
                }

                record_hash = generate_hash(row)

                if not record_exists(cur, issn, record_hash):
                    rows_to_insert.append((
                        row["issn"],
                        row["journal_title"],
                        row["publisher"],
                        row["subjects"],
                        row["country"],
                        row["open_access"],
                        row["doi_prefix"],
                        row["source"],
                        row["fetch_date"],
                        record_hash
                    ))

            if rows_to_insert:
                cur.executemany("""
                    INSERT INTO issn_metadata_fact (
                        issn, journal_title, publisher, subjects,
                        country, open_access, doi_prefix,
                        source, fetch_date, record_hash
                    )
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """, rows_to_insert)
                conn.commit()

            time.sleep(SLEEP_SECONDS)

    print("Pipeline completed successfully.")

# ------------------ ENTRY POINT ------------------
if __name__ == "__main__":
    run_pipeline()
