"""
GEV EHS Prediction API Server
==============================
FastAPI backend that connects the HTML predictions tab to Databricks MLflow models.
Secured with Azure AD (Microsoft Entra ID) authentication.

Pipeline:
  1. User authenticates via Azure AD in the browser (MSAL.js)
  2. Frontend sends CSV upload with Bearer token
  3. Backend validates token, uploads data to Databricks
  4. Runs inference notebook (theme classification → site risk scoring → SHAP explanations)
  5. Returns JSON results to frontend

Usage:
  pip install -r requirements.txt
  python api_server.py

Environment variables (in .env or exported):
  DATABRICKS_HOST       = https://adb-xxxx.azuredatabricks.net
  DATABRICKS_TOKEN      = dapi...
  AZURE_TENANT_ID       = your-azure-tenant-id
  AZURE_CLIENT_ID       = your-app-registration-client-id
  AUTH_ENABLED           = true  (set to "false" for local dev without auth)
"""

import os
import io
import json
import time
import base64
import tempfile
import logging
from pathlib import Path
from typing import Optional

import pandas as pd
import msal
from fastapi import FastAPI, UploadFile, File, HTTPException, Depends, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, JSONResponse
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from dotenv import load_dotenv
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import NotebookTask, Task, SubmitTask
from databricks.sdk.service.sql import StatementState

load_dotenv()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("gev-ehs-api")

app = FastAPI(title="GEV EHS Prediction API", version="2.0")

# ── Azure AD Configuration ────────────────────────────────────────
AZURE_TENANT_ID = os.getenv("AZURE_TENANT_ID", "")
AZURE_CLIENT_ID = os.getenv("AZURE_CLIENT_ID", "")
AUTH_ENABLED = os.getenv("AUTH_ENABLED", "false").lower() == "true"

# Disable auth if Azure AD vars not configured
if not AZURE_TENANT_ID or not AZURE_CLIENT_ID:
    AUTH_ENABLED = False

# JWKS endpoint for token validation
AZURE_AUTHORITY = f"https://login.microsoftonline.com/{AZURE_TENANT_ID}"
AZURE_ISSUER = f"https://sts.windows.net/{AZURE_TENANT_ID}/"

security = HTTPBearer(auto_error=False)

# Token validation cache
_jwks_cache = None
_jwks_cache_time = 0


async def validate_azure_token(
    credentials: Optional[HTTPAuthorizationCredentials] = Depends(security),
):
    """Validate Azure AD Bearer token. Skip if AUTH_ENABLED=false."""
    if not AUTH_ENABLED:
        return {"name": "dev-user", "email": "dev@local"}

    if not credentials:
        raise HTTPException(401, "Missing authorization token. Please sign in.")

    token = credentials.credentials

    try:
        import jwt
        from jwt import PyJWKClient

        # Fetch Azure AD public keys (cached)
        jwks_url = f"{AZURE_AUTHORITY}/discovery/v2.0/keys"
        jwk_client = PyJWKClient(jwks_url)
        signing_key = jwk_client.get_signing_key_from_jwt(token)

        # Decode and validate
        payload = jwt.decode(
            token,
            signing_key.key,
            algorithms=["RS256"],
            audience=AZURE_CLIENT_ID,
            issuer=[
                AZURE_ISSUER,
                f"https://login.microsoftonline.com/{AZURE_TENANT_ID}/v2.0",
            ],
            options={"verify_exp": True},
        )

        user = {
            "name": payload.get("name", "Unknown"),
            "email": payload.get("preferred_username", payload.get("upn", "")),
            "oid": payload.get("oid", ""),
        }
        logger.info(f"Authenticated: {user['email']}")
        return user

    except Exception as e:
        logger.warning(f"Token validation failed: {e}")
        raise HTTPException(401, f"Invalid token: {e}")


# CORS — allow all origins for now
ALLOWED_ORIGINS = ["*"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Config ────────────────────────────────────────────────────────
CATALOG = "gev_ehs"
SCHEMA = "ehs_platform"
FQN = f"{CATALOG}.{SCHEMA}"
CLUSTER_NAME = "gev_ml_ehs"
NOTEBOOK_DIR = "/Workspace/Users/pushkar.shukla@exponentia.ai/gev_ehs_platform"
INFERENCE_NOTEBOOK = f"{NOTEBOOK_DIR}/run_inference"


# ── Auth config endpoint (frontend fetches this to configure MSAL) ──
@app.get("/api/auth-config")
async def auth_config():
    """Return Azure AD config for the frontend MSAL.js setup."""
    return {
        "auth_enabled": AUTH_ENABLED,
        "client_id": AZURE_CLIENT_ID,
        "tenant_id": AZURE_TENANT_ID,
        "authority": AZURE_AUTHORITY,
        "redirect_uri": os.getenv("REDIRECT_URI", "http://localhost:8000"),
        "scopes": [f"api://{AZURE_CLIENT_ID}/access_as_user"] if AZURE_CLIENT_ID else [],
    }


def get_client():
    return WorkspaceClient(
        host=os.getenv("DATABRICKS_HOST"),
        token=os.getenv("DATABRICKS_TOKEN"),
    )


def get_cluster_id(client):
    """Find the cluster ID by name."""
    clusters = list(client.clusters.list())
    cluster = next((c for c in clusters if c.cluster_name == CLUSTER_NAME), None)
    if not cluster:
        raise HTTPException(500, f"Cluster '{CLUSTER_NAME}' not found")
    return cluster.cluster_id


# ── Serve the HTML ────────────────────────────────────────────────
STATIC_DIR = Path(__file__).parent

@app.get("/")
async def serve_html():
    return FileResponse(STATIC_DIR / "gev_ehs_value_story_v2.html")


# ── Health check ──────────────────────────────────────────────────
@app.get("/api/health")
async def health():
    """Simple health check. Returns ok if API is running (doesn't need Databricks for local dev)."""
    try:
        return {"status": "ok", "message": "API is running"}
    except Exception as e:
        return {"status": "error", "detail": str(e)}


# ── Upload & Run Pipeline ────────────────────────────────────────
@app.post("/api/predict")
async def predict(file: UploadFile = File(...), user=Depends(validate_azure_token)):
    """
    Prediction pipeline using local inference.
    Note: Databricks integration is optional. This endpoint works standalone.
    """
    # ── 1. Parse and validate CSV ──
    content = await file.read()
    try:
        df = pd.read_csv(io.BytesIO(content))
    except Exception as e:
        raise HTTPException(400, f"Could not parse CSV: {e}")

    # Normalize column names to lowercase
    df.columns = df.columns.str.lower().str.strip()

    required_cols = ["incident_task_desc", "location_nme"]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise HTTPException(400, f"Missing required columns: {missing}")

    # ── Run local inference ──
    try:
        results = run_local_inference(df)
        return JSONResponse(content={
            "status": "success",
            "upload_id": f"local_{int(time.time())}",
            "input_rows": len(df),
            "sites_scored": len(results["sites"]),
            "results": results,
        })
    except Exception as e:
        raise HTTPException(500, f"Prediction failed: {e}")


def fetch_prediction_results(client, upload_id):
    """Read prediction results from the Databricks table written by the inference notebook."""
    # Use Databricks SQL Statement Execution API
    warehouse_id = find_sql_warehouse(client)

    # Fetch site risk scores + explanations
    query = f"""
    SELECT
        site_key,
        risk_score,
        risk_tier,
        concern_count,
        themes_covered,
        blind_spot_count,
        blind_spot_themes,
        explanations,
        stopwork_rate,
        days_since_last_concern,
        concern_trend_mom
    FROM {FQN}.ml_inference_results
    WHERE upload_id = '{upload_id}'
    ORDER BY risk_score DESC
    """

    result = client.statement_execution.execute_statement(
        warehouse_id=warehouse_id,
        catalog=CATALOG,
        schema=SCHEMA,
        statement=query,
        wait_timeout="60s",
    )

    if result.status.state != StatementState.SUCCEEDED:
        raise Exception(f"SQL query failed: {result.status.error}")

    columns = [c.name for c in result.manifest.schema.columns]
    sites = []
    for row in result.result.data_array:
        site = dict(zip(columns, row))
        # Parse JSON fields
        if site.get("blind_spot_themes"):
            try:
                site["blind_spot_themes"] = json.loads(site["blind_spot_themes"])
            except:
                site["blind_spot_themes"] = site["blind_spot_themes"].split(",")
        if site.get("explanations"):
            try:
                site["explanations"] = json.loads(site["explanations"])
            except:
                site["explanations"] = [{"text": site["explanations"], "risk": True}]

        # Convert numeric strings
        for k in ["risk_score", "stopwork_rate", "concern_trend_mom"]:
            if site.get(k):
                try:
                    site[k] = float(site[k])
                except:
                    pass
        for k in ["concern_count", "themes_covered", "blind_spot_count", "days_since_last_concern"]:
            if site.get(k):
                try:
                    site[k] = int(float(site[k]))
                except:
                    pass

        sites.append(site)

    # Fetch theme gap summary across all sites
    theme_query = f"""
    SELECT
        theme_name,
        COUNT(*) as gap_count
    FROM {FQN}.ml_inference_blind_spots
    WHERE upload_id = '{upload_id}'
    GROUP BY theme_name
    ORDER BY gap_count DESC
    """

    theme_result = client.statement_execution.execute_statement(
        warehouse_id=warehouse_id,
        catalog=CATALOG,
        schema=SCHEMA,
        statement=theme_query,
        wait_timeout="30s",
    )

    theme_gaps = {}
    if theme_result.status.state == StatementState.SUCCEEDED and theme_result.result.data_array:
        for row in theme_result.result.data_array:
            theme_gaps[row[0]] = int(float(row[1]))

    return {
        "sites": sites,
        "theme_gaps": theme_gaps,
        "tier_counts": {
            "HIGH": sum(1 for s in sites if s.get("risk_tier") == "HIGH"),
            "ELEVATED": sum(1 for s in sites if s.get("risk_tier") == "ELEVATED"),
            "MODERATE": sum(1 for s in sites if s.get("risk_tier") == "MODERATE"),
            "LOW": sum(1 for s in sites if s.get("risk_tier") == "LOW"),
        },
    }


def find_sql_warehouse(client):
    """Find a running SQL warehouse to execute queries."""
    warehouses = client.warehouses.list()
    for wh in warehouses:
        if wh.state and wh.state.value == "RUNNING":
            return wh.id
    # If none running, use first one
    warehouses_list = list(client.warehouses.list())
    if warehouses_list:
        return warehouses_list[0].id
    raise Exception("No SQL warehouse found. Create one in Databricks SQL.")


# ── Fallback: lightweight predict without Databricks ──────────────
@app.post("/api/predict-local")
async def predict_local(file: UploadFile = File(...), user=Depends(validate_azure_token)):
    """
    Local fallback prediction — uses the same logic as client-side JS
    but runs server-side. Use this when Databricks is unavailable.
    """
    content = await file.read()
    try:
        df = pd.read_csv(io.BytesIO(content))
    except Exception as e:
        raise HTTPException(400, f"Could not parse CSV: {e}")

    # Normalize column names to lowercase
    df.columns = df.columns.str.lower().str.strip()

    required_cols = ["incident_task_desc", "location_nme"]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise HTTPException(400, f"Missing required columns: {missing}")

    results = run_local_inference(df)
    return JSONResponse(content={
        "status": "success",
        "upload_id": "local",
        "input_rows": len(df),
        "sites_scored": len(results["sites"]),
        "results": results,
    })


def run_local_inference(df):
    """Run the full pipeline locally using keyword classification and heuristic scoring."""
    from theme_keywords import THEME_KEYWORDS, THEMES

    # 1. Classify themes
    def classify_row(text):
        if not isinstance(text, str):
            return []
        matched = []
        for theme, pattern in THEME_KEYWORDS.items():
            import re
            if re.search(pattern, text, re.IGNORECASE):
                matched.append(theme)
        return matched

    df["_themes"] = df["incident_task_desc"].apply(classify_row)

    # 2. Aggregate by site
    sites = {}
    for _, row in df.iterrows():
        loc = row.get("location_nme", "Unknown")
        if loc not in sites:
            sites[loc] = {
                "site_key": loc,
                "concerns": [],
                "themes": set(),
                "stopwork_count": 0,
                "dates": [],
                "org": row.get("bus_nme", row.get("org_nme", "")),
            }
        sites[loc]["concerns"].append(row)
        for t in row["_themes"]:
            sites[loc]["themes"].add(t)
        ct = str(row.get("concern_type", "")).lower()
        if "stop work" in ct or "stopwork" in ct:
            sites[loc]["stopwork_count"] += 1
        dt = row.get("incident_reported_dt")
        if pd.notna(dt):
            try:
                sites[loc]["dates"].append(pd.to_datetime(dt))
            except:
                pass

    # 3. Score each site
    import datetime
    now = datetime.datetime.now()
    site_results = []

    for name, s in sites.items():
        count = len(s["concerns"])
        themes_covered = len(s["themes"])
        blind_spots = 21 - themes_covered
        blind_spot_names = [t for t in THEMES if t not in s["themes"]]
        stopwork_rate = s["stopwork_count"] / max(count, 1)
        sorted_dates = sorted(s["dates"], reverse=True)
        days_since = (now - sorted_dates[0]).days if sorted_dates else 999
        trend = 0

        # Risk score
        score = 0.04
        if days_since > 60:
            score += 0.12
        elif days_since > 30:
            score += 0.06
        score += (blind_spots / 21) * 0.08
        if count <= 2:
            score += 0.06
        elif count <= 5:
            score += 0.03
        if stopwork_rate == 0:
            score += 0.03
        if themes_covered >= 10:
            score -= 0.04
        if stopwork_rate > 0.15:
            score -= 0.03
        if count >= 20:
            score -= 0.02
        score = max(0.01, min(0.40, score))

        # Tier
        if score >= 0.15:
            tier = "HIGH"
        elif score >= 0.08:
            tier = "ELEVATED"
        elif score >= 0.05:
            tier = "MODERATE"
        else:
            tier = "LOW"

        # Explanations
        explanations = []
        if days_since > 30:
            explanations.append({"text": f"No concerns filed in {days_since} days — reporting has gone silent", "risk": True})
        if blind_spots > 10:
            explanations.append({"text": f"{blind_spots} of 21 risk themes have zero concern coverage", "risk": True})
        elif blind_spots > 5:
            explanations.append({"text": f"{blind_spots} risk themes uncovered — gaps in safety monitoring", "risk": True})
        if count < 3:
            explanations.append({"text": f"Only {count} concern(s) filed this period — very low activity", "risk": True})
        if stopwork_rate == 0 and count > 0:
            explanations.append({"text": "Zero stop-work actions — workers may not feel empowered to halt unsafe work", "risk": True})
        if themes_covered >= 10:
            explanations.append({"text": f"{themes_covered} risk themes actively monitored — broad safety awareness", "risk": False})
        if stopwork_rate > 0.15:
            explanations.append({"text": f"{int(stopwork_rate*100)}% stop-work rate — strong safety culture signal", "risk": False})
        if count >= 20:
            explanations.append({"text": f"{count} concerns filed — active reporting culture", "risk": False})
        if not explanations:
            explanations.append({"text": "Moderate activity levels with some monitoring gaps", "risk": True})

        site_results.append({
            "site_key": name,
            "risk_score": round(score, 4),
            "risk_tier": tier,
            "concern_count": count,
            "themes_covered": themes_covered,
            "blind_spot_count": blind_spots,
            "blind_spot_themes": blind_spot_names,
            "explanations": explanations,
            "stopwork_rate": round(stopwork_rate, 3),
            "days_since_last_concern": days_since,
            "concern_trend_mom": trend,
        })

    site_results.sort(key=lambda x: x["risk_score"], reverse=True)

    # Theme gap counts
    theme_gaps = {}
    for s in site_results:
        for t in s["blind_spot_themes"]:
            theme_gaps[t] = theme_gaps.get(t, 0) + 1

    return {
        "sites": site_results,
        "theme_gaps": theme_gaps,
        "tier_counts": {
            "HIGH": sum(1 for s in site_results if s["risk_tier"] == "HIGH"),
            "ELEVATED": sum(1 for s in site_results if s["risk_tier"] == "ELEVATED"),
            "MODERATE": sum(1 for s in site_results if s["risk_tier"] == "MODERATE"),
            "LOW": sum(1 for s in site_results if s["risk_tier"] == "LOW"),
        },
    }


if __name__ == "__main__":
    import uvicorn
    print("Starting GEV EHS Prediction API on http://localhost:8000")
    print("Open http://localhost:8000 to view the presentation")
    uvicorn.run(app, host="0.0.0.0", port=8000)
