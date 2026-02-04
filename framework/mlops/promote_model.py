import os
from databricks import sql

MODEL_NAME = os.getenv("MODEL_NAME", "customer_risk")
REGISTRY_TABLE = os.getenv("REGISTRY_TABLE", "dataops.model_registry")

def main():
   host = os.environ["DATABRICKS_HOST"]
   http_path = os.environ["DATABRICKS_HTTP_PATH"]
   token = os.environ["DATABRICKS_TOKEN"]

   with sql.connect(server_hostname=host, http_path=http_path, access_token=token) as conn:
       with conn.cursor() as cur:
           # 1) obtener última versión registrada
           cur.execute(f"""
             SELECT model_version
             FROM {REGISTRY_TABLE}
             WHERE model_name = ? AND status = 'REGISTERED'
             ORDER BY created_at DESC
             LIMIT 1
           """, (MODEL_NAME,))
           row = cur.fetchone()
           if not row:
               raise RuntimeError("No REGISTERED model found to promote.")
           new_version = row[0]

           # 2) archivar el PROD actual (si existe)
           cur.execute(f"""
             UPDATE {REGISTRY_TABLE}
             SET status = 'ARCHIVED'
             WHERE model_name = ? AND status = 'PROD'
           """, (MODEL_NAME,))

           # 3) promover a PROD
           cur.execute(f"""
             UPDATE {REGISTRY_TABLE}
             SET status = 'PROD'
             WHERE model_name = ? AND model_version = ?
           """, (MODEL_NAME, new_version))

   print("✅ Promoted to PROD:", new_version)

if __name__ == "__main__":
   main()