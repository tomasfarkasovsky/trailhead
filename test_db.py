import os
import mysql.connector

conn = mysql.connector.connect(
    host=os.environ["DB_HOST"],
    port=int(os.environ.get("DB_PORT", "3306")),
    user=os.environ["DB_USER"],
    password=os.environ["DB_PASS"],
    database=os.environ["DB_NAME"],
    ssl_ca=os.environ.get("DB_SSL_CA", "ca.pem"),
    ssl_verify_cert=True,
)

cur = conn.cursor()
cur.execute("SELECT 1")
print("SELECT 1 =>", cur.fetchone())

cur.execute("SHOW STATUS LIKE 'Ssl_cipher'")
print("SSL =>", cur.fetchone())

cur.close()
conn.close()
print("✅ Connection OK")
