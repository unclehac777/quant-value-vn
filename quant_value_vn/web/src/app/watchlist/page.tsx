import { getWatchlist, getRankings } from "@/lib/api";
import WatchlistClient from "./WatchlistClient";

export const dynamic = 'force-dynamic';

export default async function WatchlistPage() {
  const [watchlist, rankings] = await Promise.all([
    getWatchlist(),
    getRankings(),
  ]);

  return (
    <div className="max-w-7xl mx-auto space-y-6">
      <div>
        <h1 className="text-3xl font-bold tracking-tight text-white">Watchlist & Portfolio</h1>
        <p className="text-gray-400 mt-2">Track your holdings and saved screening ideas</p>
      </div>

      <WatchlistClient initialWatchlist={watchlist} rankings={rankings} />
    </div>
  );
}
