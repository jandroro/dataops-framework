import os
import json
from datetime import datetime, timezone
from pathlib import Path

from databricks import sql


ARTIFACTS_DIR = Path("framework/mlops/artifacts")
METRICS_FILE = ARTIFACTS_DIR / "metrics.json"

MODEL_NAME = os.getenv("MODEL_NAME", "customer_risk")
REGISTRY_TABLE = os.getenv("REGISTRY_TABLE", "dataops.model_registry")

GIT_SHA = os.getenv("GIT_SHA", "unknown")
RUN_ID = os.getenv("GITHUB_RUN_ID", "unknown")
REPO = os.getenv("GITHUB_REPOSITORY", "unknown")


def main():
   if not METRICS_FILE.exists():
       raise FileNotFoundError(f"metrics.json not found at {METRICS_FILE}")

   metrics = json.loads(METRICS_FILE.read_text(encoding="utf-8"))

   # version simple y audit-friendly
   short_sha = GIT_SHA[:7]
   ts = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
   model_version = f"v{ts}-{short_sha}"

   created_at = datetime.now(timezone.utc).isoformat()

   # referencia lógica al artefacto (el run_id identifica el build)
   artifact_uri = f"github://{REPO}/actions/runs/{RUN_ID}#mlops-artifacts"

   host = os.environ["DATABRICKS_HOST"]
   http_path = os.environ["DATABRICKS_HTTP_PATH"]
   token = os.environ["DATABRICKS_TOKEN"]

   insert_sql = f"""
   INSERT INTO {REGISTRY_TABLE}
   (model_name, model_version, created_at, git_sha, data_source, metrics_json, artifact_uri, status)
   VALUES (?, ?, ?, ?, ?, ?, ?, ?)
   """

   data_source = os.getenv("SILVER_TABLE", "dataops.silver_customers_v2")

   with sql.connect(server_hostname=host, http_path=http_path, access_token=token) as conn:
       with conn.cursor() as cur:
           cur.execute(
               insert_sql,
               (
                   MODEL_NAME,
                   model_version,
                   created_at,
                   GIT_SHA,
                   data_source,
                   json.dumps(metrics),
                   artifact_uri,
                   "REGISTERED",
               ),
           )

   print("✅ Model registered")
   print(f"📌 model_name={MODEL_NAME}")
   print(f"🏷️ model_version={model_version}")
   print(f"🔗 artifact_uri={artifact_uri}")


if __name__ == "__main__":
   main()
