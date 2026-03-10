import { getRankings, getRuns } from "@/lib/api";
import DashboardClient from "./DashboardClient";

export const dynamic = 'force-dynamic';

export default async function Home() {
  const [rankings, runs] = await Promise.all([
    getRankings(),
    getRuns()
  ]);

  return (
    <div className="max-w-7xl mx-auto space-y-6">
      <div>
        <h1 className="text-3xl font-bold tracking-tight text-white">Dashboard</h1>
        <p className="text-gray-400 mt-2">Quantitative Value Screener - Vietnam Market</p>
      </div>
      
      <DashboardClient rankings={rankings} runs={runs} />
    </div>
  );
}
