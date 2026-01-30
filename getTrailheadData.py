import os
import requests
import pandas as pd
from datetime import datetime
import mysql.connector

GRAPHQL_URL = "https://profile.api.trailhead.com/graphql"

GRAPHQL_QUERY = """
query GetUserCertifications($slug: String, $hasSlug: Boolean!) {
  profile(slug: $slug) @include(if: $hasSlug) {
    __typename
    id
    ... on PublicProfile {
      credential {
        certifications {
          title
          dateCompleted
          dateExpired
          product
          publicDescription
          status {
            title
            expired
            date
          }
        }
      }
    }
  }
}
"""

def get_db_connection():
    return mysql.connector.connect(
        host=os.environ["DB_HOST"],
        port=int(os.environ.get("DB_PORT", "3306")),
        user=os.environ["DB_USER"],
        password=os.environ["DB_PASS"],
        database=os.environ["DB_NAME"],
        connection_timeout=20,
    )

def load_profiles_from_db(conn):
    sql = "SELECT username FROM trailhead_profiles WHERE active = 1"
    with conn.cursor() as cur:
        cur.execute(sql)
        return [row[0] for row in cur.fetchall()]

def fetch_certifications(username, start_date):
    headers = {"Content-Type": "application/json"}
    payload = {
        "operationName": "GetUserCertifications",
        "variables": {"hasSlug": True, "slug": username},
        "query": GRAPHQL_QUERY
    }

    try:
        response = requests.post(GRAPHQL_URL, json=payload, headers=headers, timeout=30)
    except Exception as e:
        return {"Username": username, "Error": f"Request failed: {e}"}

    if response.status_code != 200:
        return {"Username": username, "Error": f"HTTP {response.status_code}"}

    try:
        data = response.json()
    except Exception as e:
        return {"Username": username, "Error": f"Invalid JSON: {e}"}

    if not isinstance(data, dict):
        return {"Username": username, "Error": "Invalid response format"}

    if data.get("data") is None:
        return {"Username": username, "Error": "No public profile found"}

    profile = data["data"].get("profile", {})
    if not profile:
        return {"Username": username, "Error": "No public profile found"}

    certifications = profile.get("credential", {}).get("certifications", []) or []

    filtered = []
    for c in certifications:
        dc = c.get("dateCompleted")
        if not dc:
            continue
        try:
            if datetime.strptime(dc, "%Y-%m-%d") >= start_date:
                filtered.append(c)
        except Exception:
            # ignore malformed dates
            pass

    return {
        "Username": username,
        "Certifications": "\n".join(
            f"{c.get('title', '')} ({c.get('dateCompleted', '')})" for c in filtered
        ),
        "Total Certifications overall": len(certifications),
        "Total Certifications year": len(filtered)
    }

def upsert_results(conn, year, results):
    sql = """
    INSERT INTO trailhead_certs_yearly
      (username, year, certifications, total_certifications_overall, total_certifications_year)
    VALUES
      (%s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
      certifications = VALUES(certifications),
      total_certifications_overall = VALUES(total_certifications_overall),
      total_certifications_year = VALUES(total_certifications_year),
      updated_at = CURRENT_TIMESTAMP
    """
    with conn.cursor() as cur:
        for r in results:
            if r.get("Error"):
                # optional: store errors somewhere else; for now we just skip
                continue
            cur.execute(sql, (
                r["Username"],
                year,
                r.get("Certifications", ""),
                int(r.get("Total Certifications overall", 0)),
                int(r.get("Total Certifications year", 0)),
            ))
    conn.commit()

def main():
    # Use current year automatically
    year = datetime.utcnow().year
    start_date = datetime(year, 1, 1)

    conn = get_db_connection()
    try:
        profiles = load_profiles_from_db(conn)
        if not profiles:
            print("No active profiles found in DB.")
            return

        results = [fetch_certifications(u, start_date) for u in profiles]

        # Save also to CSV artifact (optional)
        df = pd.DataFrame(results)
        csv_filename = f"trailhead_certs_{year}.csv"
        df.to_csv(csv_filename, index=False)
        print(f"✅ CSV saved to '{csv_filename}'")

        upsert_results(conn, year, results)
        print(f"✅ MySQL updated for {len([r for r in results if not r.get('Error')])} users")

        # Print errors to logs (so you see them in Actions)
        for r in results:
            if r.get("Error"):
                print(f"⚠️ {r['Username']}: {r['Error']}")

    finally:
        conn.close()

if __name__ == "__main__":
    main()
