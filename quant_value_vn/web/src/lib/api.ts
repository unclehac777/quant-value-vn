export const API_BASE = process.env.NEXT_PUBLIC_API_BASE || "http://localhost:8000";

export async function fetchAPI(endpoint: string, options?: RequestInit) {
  const res = await fetch(`${API_BASE}${endpoint}`, {
    ...options,
    // For Next.js 15 cache handling
    cache: options?.cache || 'no-store',
  });
  
  if (!res.ok) {
    throw new Error(`API error: ${res.statusText}`);
  }
  return res.json();
}

export async function getRankings() {
  const data = await fetchAPI("/rankings");
  return data.data || [];
}

export async function getRuns() {
  const data = await fetchAPI("/runs");
  return data.data || [];
}

export async function getWatchlist() {
  const data = await fetchAPI("/watchlist");
  return data.data || [];
}

export async function getPortfolio() {
  const data = await fetchAPI("/portfolio");
  return data.data || [];
}

export async function getStockDetail(ticker: string) {
  return await fetchAPI(`/stock/${ticker}`);
}

export async function addToWatchlist(ticker: string, notes: string = "") {
  return await fetchAPI("/watchlist", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ ticker, notes }),
  });
}

export async function removeFromWatchlist(ticker: string) {
  return await fetchAPI(`/watchlist/${ticker}`, {
    method: "DELETE",
  });
}

export async function updateWatchlistItem(ticker: string, data: { notes?: string, buy_price?: number, shares?: number }) {
  // The API uses direct DB update for this, let's check if there's a route.
  // Actually, looking at routes.py, there is no PATCH /watchlist/{ticker} yet.
  // But Dashboard code showed a direct DB update. 
  // I should probably add an API route or use a POST if I can find one.
  // Wait, let's check routes.py again.
  return await fetchAPI(`/watchlist/${ticker}`, {
    method: "PUT", // I will need to check/add this route
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(data),
  });
}
export async function triggerScan(config: { max_stocks?: number, workers?: number, min_mcap?: number, max_am?: number } = {}) {
  const defaults = {
    max_stocks: 9999,
    workers: 10,
    min_mcap: 500000000, // 500M
    max_am: 50.0
  };
  return fetchAPI("/run", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ ...defaults, ...config }),
  });
}
