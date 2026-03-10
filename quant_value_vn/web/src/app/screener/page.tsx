import { getRankings } from "@/lib/api";
import ScreenerClient from "./ScreenerClient";

export const dynamic = 'force-dynamic';

export default async function Screener() {
  const rankings = await getRankings();

  return (
    <div className="max-w-7xl mx-auto space-y-6">
      <div>
        <h1 className="text-3xl font-bold tracking-tight text-white">Screening Results</h1>
        <p className="text-gray-400 mt-2">Filter and analyze quantitative value metrics</p>
      </div>
      
      <ScreenerClient data={rankings} />
    </div>
  );
}
