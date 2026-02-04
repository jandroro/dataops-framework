import os
from databricks import sql

HOST = os.environ["DATABRICKS_HOST"]
HTTP_PATH = os.environ["DATABRICKS_HTTP_PATH"]
TOKEN = os.environ["DATABRICKS_TOKEN"]

SQL_FILE = os.environ.get("SQL_FILE")
SILVER_TABLE = os.environ.get("SILVER_TABLE", "dataops.silver_customers_v2")  # default

def main():
    if not SQL_FILE:
        raise ValueError("Missing SQL_FILE env var")

    with open(SQL_FILE, "r", encoding="utf-8") as f:
        script = f.read()

    # ✅ Reemplazo de placeholder (clave del error)
    script = script.replace("__SILVER_TABLE__", SILVER_TABLE)

    # Split básico por ';' (suficiente para taller)
    statements = [s.strip() for s in script.split(";") if s.strip()]

    print(f"✅ Running SQL_FILE={SQL_FILE}")
    print(f"✅ SILVER_TABLE={SILVER_TABLE}")
    print(f"✅ Statements={len(statements)}")

    with sql.connect(server_hostname=HOST, http_path=HTTP_PATH, access_token=TOKEN) as conn:
        with conn.cursor() as cur:
            for i, st in enumerate(statements, start=1):
                preview = st.replace("\n", " ")[:160]
                print(f"--- Executing statement {i}/{len(statements)}: {preview} ...")
                cur.execute(st)

    print("✅ SQL execution completed")

if __name__ == "__main__":
    main()
