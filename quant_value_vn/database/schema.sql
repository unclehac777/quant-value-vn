-- ============================================================================
-- Vietnam Quantitative Value Stock Screener — Supabase Schema
-- Based on Tobias Carlisle & Wesley Gray "Quantitative Value" framework
-- Run this in your Supabase SQL Editor (https://supabase.com/dashboard)
-- ============================================================================

-- Screening runs metadata
CREATE TABLE IF NOT EXISTS screening_runs (
    id            BIGSERIAL PRIMARY KEY,
    run_date      TEXT        NOT NULL,
    total_stocks  INTEGER     NOT NULL DEFAULT 0,
    passed_filter INTEGER     NOT NULL DEFAULT 0,
    max_stocks    INTEGER     NOT NULL DEFAULT 150,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Per-stock results for each run
CREATE TABLE IF NOT EXISTS screening_results (
    id                   BIGSERIAL PRIMARY KEY,
    run_id               BIGINT      NOT NULL REFERENCES screening_runs(id) ON DELETE CASCADE,
    ticker               TEXT        NOT NULL,
    combined_rank        INTEGER,
    acquirers_multiple   DOUBLE PRECISION,
    ebit_ev              DOUBLE PRECISION,
    quality_score        DOUBLE PRECISION,
    -- Fraud detection (Beneish M-Score)
    beneish_mscore       DOUBLE PRECISION,
    probm                DOUBLE PRECISION,
    -- Fundamentals
    market_cap           DOUBLE PRECISION,
    enterprise_value     DOUBLE PRECISION,
    ebit                 DOUBLE PRECISION,
    revenue              DOUBLE PRECISION,
    -- Profitability
    roa                  DOUBLE PRECISION,
    roe                  DOUBLE PRECISION,
    roic                 DOUBLE PRECISION,
    gross_profitability  DOUBLE PRECISION,
    -- Earnings quality
    accruals             DOUBLE PRECISION,
    cfo_to_assets        DOUBLE PRECISION,
    operating_cash_flow  DOUBLE PRECISION,
    -- Valuation
    fcf_yield            DOUBLE PRECISION,
    pe                   DOUBLE PRECISION,
    pb                   DOUBLE PRECISION,
    debt_equity          DOUBLE PRECISION,
    gross_margin         DOUBLE PRECISION,
    net_margin           DOUBLE PRECISION,
    -- Rankings
    value_rank           INTEGER,
    quality_rank         INTEGER,
    -- Price & display
    price                DOUBLE PRECISION,
    eps                  DOUBLE PRECISION,
    market_cap_b         DOUBLE PRECISION,
    ev_b                 DOUBLE PRECISION
);

CREATE INDEX IF NOT EXISTS idx_results_run    ON screening_results(run_id);
CREATE INDEX IF NOT EXISTS idx_results_ticker ON screening_results(ticker);
CREATE INDEX IF NOT EXISTS idx_results_rank   ON screening_results(combined_rank);

-- Watchlist / portfolio
CREATE TABLE IF NOT EXISTS watchlist (
    id         BIGSERIAL PRIMARY KEY,
    ticker     TEXT        NOT NULL UNIQUE,
    notes      TEXT        DEFAULT '',
    buy_price  DOUBLE PRECISION,
    shares     DOUBLE PRECISION,
    added_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Portfolio history (for tracking model portfolio over time)
CREATE TABLE IF NOT EXISTS portfolio_history (
    id         BIGSERIAL PRIMARY KEY,
    run_id     BIGINT      NOT NULL REFERENCES screening_runs(id) ON DELETE CASCADE,
    ticker     TEXT        NOT NULL,
    weight     DOUBLE PRECISION DEFAULT 0.0333,
    rank       INTEGER,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_portfolio_run ON portfolio_history(run_id);
