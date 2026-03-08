"""
FastAPI application for the Quantitative Value screener.

Provides a thin API layer between the database and the Streamlit dashboard.
Streamlit should NOT query the database directly — it goes through this API.

Endpoints:
- GET /rankings           → latest top-30 ranked stocks
- GET /rankings/{run_id}  → results for a specific run
- GET /runs               → list of all screening runs
- GET /stock/{ticker}     → detail for one ticker
- GET /portfolio          → latest model portfolio
- GET /watchlist          → watchlist
- POST /watchlist         → add to watchlist
- DELETE /watchlist/{tk}  → remove from watchlist
- POST /run              → trigger a new pipeline run

Run: uvicorn quant_value_vn.app.main:app --host 0.0.0.0 --port 8000
"""

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from quant_value_vn.app.routes import router
from quant_value_vn.config import API_HOST, API_PORT

app = FastAPI(
    title="VN Quantitative Value API",
    description="Tobias Carlisle & Wesley Gray Quantitative Value framework — Vietnam market",
    version="2.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(router)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host=API_HOST, port=API_PORT)
