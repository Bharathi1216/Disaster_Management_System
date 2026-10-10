
import os
import certifi
from dotenv import load_dotenv
from pymongo import MongoClient
from neo4j import GraphDatabase, TrustCustomCAs

load_dotenv()

MONGO_URI = os.environ["MONGO_URI"]
NEO4J_URI = os.environ["NEO4J_URI"]
NEO4J_USERNAME = os.environ.get("NEO4J_USERNAME", "neo4j")
NEO4J_PASSWORD = os.environ["NEO4J_PASSWORD"]

MONGO_DB = "smart_disaster_db"
MONGO_COLLECTION = "disasters_normalized"
BATCH_SIZE = 500


def to_number(value):
    """Convert numeric values safely; missing/invalid values stay None."""
    if value is None or isinstance(value, bool):
        return None

    if isinstance(value, (int, float)):
        return value

    if isinstance(value, str):
        value = value.strip().replace(",", "")
        if not value:
            return None
        try:
            number = float(value)
            return int(number) if number.is_integer() else number
        except ValueError:
            return None

    return None


def clean_text(value):
    if value is None:
        return None
    value = str(value).strip()
    return value or None


def split_associated_types(value):
    if not value:
        return []

    if isinstance(value, list):
        values = value
    else:
        values = str(value).split("|")

    return sorted({
        item.strip()
        for item in values
        if item and item.strip()
    })


def transform(doc):
    event_key = clean_text(doc.get("eventKey") or doc.get("DisNo"))
    country = clean_text(doc.get("Country"))
    disaster_type = clean_text(doc.get("Disaster Type"))
    start_year = to_number(doc.get("Start Year"))

    # Do not import records without the minimum graph identifiers.
    if not event_key or not country or not disaster_type:
        return None

    if start_year is None:
        return None

    iso = clean_text(doc.get("ISO"))

    return {
        "eventKey": event_key,
        "country": country,
        # ISO is preferred as the stable country identifier.
        # Country name is the fallback if ISO is absent.
        "countryKey": iso or country,
        "iso": iso,
        "disasterType": disaster_type,
        "startYear": int(start_year),
        "totalDeaths": to_number(doc.get("Total Deaths")),
        "totalAffected": to_number(doc.get("Total Affected")),
        "origin": clean_text(doc.get("Origin")),
        "region": clean_text(doc.get("Region")),
        "subregion": clean_text(doc.get("Subregion")),
        "appeal": clean_text(doc.get("Appeal")),
        "declaration": clean_text(doc.get("Declaration")),
        "associatedTypes": split_associated_types(
            doc.get("Associated Types")
        ),
    }


UPSERT_QUERY = """
UNWIND $rows AS row

MERGE (r:DisasterRecord {eventKey: row.eventKey})
SET r.startYear = row.startYear,
    r.totalDeaths = row.totalDeaths,
    r.totalAffected = row.totalAffected,
    r.origin = row.origin,
    r.appeal = row.appeal,
    r.declaration = row.declaration

MERGE (c:Country {key: row.countryKey})
SET c.name = row.country,
    c.iso = row.iso,
    c.region = row.region,
    c.subregion = row.subregion

MERGE (t:DisasterType {name: row.disasterType})
MERGE (y:Year {value: row.startYear})

MERGE (r)-[:OCCURRED_IN]->(c)
MERGE (r)-[:HAS_TYPE]->(t)
MERGE (r)-[:IN_YEAR]->(y)

WITH row, t
UNWIND row.associatedTypes AS secondaryName
WITH DISTINCT t, secondaryName
WHERE secondaryName <> t.name
MERGE (secondary:DisasterType {name: secondaryName})
MERGE (t)-[:ASSOCIATED_WITH]->(secondary)
"""

# Separate core query ensures records with no associated types
# are still imported.
CORE_QUERY = """
UNWIND $rows AS row

MERGE (r:DisasterRecord {eventKey: row.eventKey})
SET r.startYear = row.startYear,
    r.totalDeaths = row.totalDeaths,
    r.totalAffected = row.totalAffected,
    r.origin = row.origin,
    r.appeal = row.appeal,
    r.declaration = row.declaration

MERGE (c:Country {key: row.countryKey})
SET c.name = row.country,
    c.iso = row.iso,
    c.region = row.region,
    c.subregion = row.subregion

MERGE (t:DisasterType {name: row.disasterType})
MERGE (y:Year {value: row.startYear})

MERGE (r)-[:OCCURRED_IN]->(c)
MERGE (r)-[:HAS_TYPE]->(t)
MERGE (r)-[:IN_YEAR]->(y)
"""

ASSOCIATED_QUERY = """
UNWIND $rows AS row
UNWIND row.associatedTypes AS secondaryName
MATCH (t:DisasterType {name: row.disasterType})
WHERE secondaryName <> row.disasterType
MERGE (secondary:DisasterType {name: secondaryName})
MERGE (t)-[:ASSOCIATED_WITH]->(secondary)
"""

def main():
    mongo_client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=10000)
    neo4j_driver = GraphDatabase.driver(
        NEO4J_URI.replace("neo4j+s://", "neo4j://", 1),
        auth=(NEO4J_USERNAME, NEO4J_PASSWORD),
        encrypted=True,
        trusted_certificates=TrustCustomCAs(certifi.where()),
    )

    imported = 0
    skipped = 0

    try:
        mongo_client.admin.command("ping")
        neo4j_driver.verify_connectivity()

        collection = mongo_client[MONGO_DB][MONGO_COLLECTION]

        with neo4j_driver.session() as session:
            batch = []

            for doc in collection.find({}):
                row = transform(doc)

                if row is None:
                    skipped += 1
                    continue

                batch.append(row)

                if len(batch) >= BATCH_SIZE:
                    session.run(CORE_QUERY, rows=batch).consume()
                    session.run(ASSOCIATED_QUERY, rows=batch).consume()
                    imported += len(batch)
                    print("Processed:", imported)
                    batch = []

            if batch:
                session.run(CORE_QUERY, rows=batch).consume()
                session.run(ASSOCIATED_QUERY, rows=batch).consume()
                imported += len(batch)

        print("\nImport finished.")
        print("Records processed:", imported)
        print("Records skipped:", skipped)

    finally:
        mongo_client.close()
        neo4j_driver.close()


if __name__ == "__main__":
    main()
