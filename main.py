from fastapi import FastAPI, Depends, HTTPException, Header
from pydantic import BaseModel
from typing import List, Optional
from app.config import settings
from app.clickhouse_client import get_client
from app.sources.marinetraffic import get_port_congestion
# import other sources...

app = FastAPI(title="Port & Logistics Disruption Forecaster")

class ForecastQuery(BaseModel):
    ports: List[str]
    horizon_days: int = 30
    keywords: Optional[List[str]] = None

class ForecastSignal(BaseModel):
    source: str
    type: str
    value: str
    impact: float

class ForecastResult(BaseModel):
    port: str
    country: str
    disruption_risk_score: float
    risk_level: str
    predicted_delay_days: Optional[int]
    confidence: float
    contributing_signals: List[ForecastSignal]
    recommendations: List[str]

@app.post("/api/v1/port-disruption/forecast")
async def forecast_disruption(
    query: ForecastQuery,
    x_api_key: str = Header(None)
):
    if x_api_key != settings.API_KEY:
        raise HTTPException(401, "Invalid API key")

    results = []
    for port in query.ports:
        # Fetch signals (parallel in real version)
        congestion = get_port_congestion(port)
        gdelt_signals = get_gdelt_signals(port, query.keywords or [])
        # acled_signals = ...
        # news_signals = ...

        # Simple scoring (expand later)
        score = congestion.get("risk", 0) + gdelt_signals.get("risk", 0)
        level = "High" if score > 60 else "Medium" if score > 30 else "Low"

        results.append(ForecastResult(
            port=port,
            country="Unknown",  # resolve via lookup
            disruption_risk_score=round(score, 1),
            risk_level=level,
            predicted_delay_days=int(score / 5) if score > 20 else None,
            confidence=0.85,
            contributing_signals=[
                ForecastSignal(source="MarineTraffic", type="Congestion", value=f"{congestion.get('vessels_waiting', 0)} vessels", impact=congestion.get("risk", 0))
                # add others
            ],
            recommendations=["Reroute via alternative port", "Increase buffer stock"] if score > 50 else ["Monitor"]
        ))

    return {"forecasts": results, "as_of": datetime.datetime.utcnow().isoformat() + "Z"}
