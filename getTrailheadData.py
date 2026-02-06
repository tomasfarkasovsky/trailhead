import os
import sys
import requests
import mysql.connector
from datetime import datetime, date
from typing import Optional, Dict, Any, List

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


# -----------------------------
# Utilities
# -----------------------------
def require_env(*keys: str) -> None:
    missing = [k for k in keys if not os.getenv(k)]
    if missing:
        raise RuntimeError(f"Missing required env vars: {', '.join(missing)}")


def parse_iso_date(d: Optional[str]) -> Optional[date]:
    if not d:
        return None
    try:
        return datetime.strptime(d[:10], "%Y-%m-%d").date()
    except Exception:
        return None


# -----------------------------
# DB
# -----------------------------
def get_db_connection():
    require_env("DB_HOST", "DB_PORT", "DB_NAME", "DB_USER", "DB_PASS")

    kwargs = dict(
        host=os.environ["DB_HOST"],
        port=int(os.environ.get("DB_PORT", "3306")),
        user=os.environ["DB_USER"],
        password=os.environ["DB_PASS"],
        database=os.environ["DB_NAME"],
        connection_timeout=20,
    )

    # TLS / SSL (optional)
    ssl_ca = os.environ.get("DB_SSL_CA")
    if ssl_ca and os.path.exists(ssl_ca):
        kwargs["ssl_ca"] = ssl_ca
        kwargs["ssl_verify_cert"] = True

    return mysql.connector.connect(**kwargs)


def load_profiles_from_db(conn) -> List[str]:
    sql = "SELECT username FROM trailhead_user WHERE active = 1"
    with conn.cursor() as cur:
        cur.execute(sql)
        return [row[0] for row in cur.fetchall()]


def set_session_limits(conn) -> None:
    # Avoid GROUP_CONCAT truncation when exporting from the view
    with conn.cursor() as cur:
        cur.execute("SET SESSION group_concat_max_len = 100000")


def upsert_user(conn, name: str, username: str) -> int:
    """
    Do NOT overwrite existing 'name' if it already exists.
    Fill it only when NULL or empty string.
    """
    sql = """
    INSERT INTO trailhead_user (name, username)
    VALUES (%s, %s)
    ON DUPLICATE KEY UPDATE
      name = CASE
        WHEN trailhead_user.name IS NULL OR trailhead_user.name = '' THEN VALUES(name)
        ELSE trailhead_user.name
      END,
      updated_at = CURRENT_TIMESTAMP
    """
    with conn.cursor() as cur:
        cur.execute(sql, (name, username))
        cur.execute("SELECT id FROM trailhead_user WHERE username=%s", (username,))
        return cur.fetchone()[0]


def upsert_cert(conn, title: str, product: Optional[str]) -> int:
    sql = """
    INSERT INTO trailhead_cert (title, product)
    VALUES (%s, %s)
    ON DUPLICATE KEY UPDATE
      title = VALUES(title),
      product = VALUES(product)
    """
    with conn.cursor() as cur:
        cur.execute(sql, (title, product))
        cur.execute(
            "SELECT id FROM trailhead_cert WHERE title=%s AND (product <=> %s)",
            (title, product),
        )
        return cur.fetchone()[0]


def upsert_user_cert(conn, user_id: int, cert_id: int, date_completed: date, date_expired: Optional[date]) -> None:
    """
    Upsert the relation. Only bump updated_at when date_expired actually changes.
    (If you don't care about updated_at churn, you can simplify this back.)
    """
    sql = """
    INSERT INTO trailhead_user_cert (user_id, cert_id, date_completed, date_expired)
    VALUES (%s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
      date_expired = VALUES(date_expired),
      updated_at = IF(VALUES(date_expired) <=> date_expired, updated_at, CURRENT_TIMESTAMP)
    """
    with conn.cursor() as cur:
        cur.execute(sql, (user_id, cert_id, date_completed, date_expired))


# -----------------------------
# Trailhead fetch
# -----------------------------
def fetch_certifications(username: str) -> Dict[str, Any]:
    headers = {"Content-Type": "application/json"}
    payload = {
        "operationName": "GetUserCertifications",
        "variables": {"hasSlug": True, "slug": username},
        "query": GRAPHQL_QUERY,
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

    profile = (data.get("data") or {}).get("profile")
    if not profile:
        return {"Username": username, "Error": "No public profile found"}

    credential = profile.get("credential") or {}
    certifications = credential.get("certifications") or []

    norm: List[Dict[str, Any]] = []
    for c in certifications:
        title = (c.get("title") or "").strip()
        if not title:
            continue

        dc = parse_iso_date(c.get("dateCompleted"))
        if not dc:
            continue

        de = parse_iso_date(c.get("dateExpired"))
        product = c.get("product")

        norm.append(
            {
                "title": title,
                "product": product,
                "dateCompleted": dc,
                "dateExpired": de,
            }
        )

    return {"Username": username, "CertificationsRaw": norm}


def sync_user_to_db(conn, username: str, certs_raw: List[Dict[str, Any]]) -> None:
    # If you later fetch a real display name, replace name=username with that value.
    user_id = upsert_user(conn, name=username, username=username)

    for c in certs_raw:
        cert_id = upsert_cert(conn, c["title"], c.get("product"))
        upsert_user_cert(conn, user_id, cert_id, c["dateCompleted"], c.get("dateExpired"))


# -----------------------------
# Main
# -----------------------------
def main() -> int:
    conn = get_db_connection()
    try:
        set_session_limits(conn)

        profiles = load_profiles_from_db(conn)
        if not profiles:
            print("No active profiles found in DB.")
            return 0

        ok = 0
        errors = 0

        for username in profiles:
            r = fetch_certifications(username)
            if r.get("Error"):
                errors += 1
                print(f"⚠️ {username}: {r['Error']}")
                continue

            sync_user_to_db(conn, username, r["CertificationsRaw"])
            ok += 1

        conn.commit()
        print(f"✅ MySQL synced for {ok} users")

        if errors:
            print(f"⚠️ Completed with {errors} errors (see logs above)")

        return 0

    finally:
        conn.close()


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as e:
        print(f"❌ Failed: {e}", file=sys.stderr)
        raise
