"use client";

import { useMemo } from "react";
import { 
  ScatterChart, 
  Scatter, 
  XAxis, 
  YAxis, 
  ZAxis, 
  CartesianGrid, 
  Tooltip, 
  ResponsiveContainer,
  Cell
} from "recharts";
import { motion } from "framer-motion";
import { BadgeDelta } from "@tremor/react";

const CustomTooltip = ({ active, payload }: any) => {
  if (active && payload && payload.length) {
    const data = payload[0].payload;
    return (
      <div className="glass-panel p-4 rounded-xl border border-sky-500/30">
        <p className="text-xl font-black text-white">{data.ticker}</p>
        <div className="mt-2 space-y-1 text-xs font-mono">
          <p className="text-slate-400">AM Multiple: <span className="text-sky-400">{data.am.toFixed(2)}x</span></p>
          <p className="text-slate-400">Quality Score: <span className="text-emerald-400">{data.quality}</span></p>
          <p className="text-slate-400">Market Cap: <span className="text-slate-200">{data.mcap.toFixed(1)}B</span></p>
        </div>
      </div>
    );
  }
  return null;
};

export default function DashboardClient({ rankings, runs }: { rankings: any[], runs: any[] }) {
  const latestRun = runs.length > 0 ? runs[0] : null;
  const topPick = rankings.length > 0 ? rankings[0] : null;

  const scatterData = useMemo(() => rankings.map(r => ({
    ticker: r.ticker,
    quality: r.quality_score,
    am: r.acquirers_multiple,
    mcap: r.market_cap_b || 0,
  })), [rankings]);

  const cleanCount = rankings.filter(r => r.beneish_mscore <= -1.78).length;

  const containerVariants = {
    hidden: { opacity: 0, y: 20 },
    visible: { 
      opacity: 1, 
      y: 0,
      transition: { duration: 0.6, staggerChildren: 0.1 }
    }
  };

  const itemVariants = {
    hidden: { opacity: 0, scale: 0.95 },
    visible: { opacity: 1, scale: 1 }
  };

  return (
    <motion.div 
      initial="hidden"
      animate="visible"
      variants={containerVariants}
      className="space-y-8"
    >
      {/* Premium KPI Grid */}
      <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-6">
        {[
          { label: "Total Screened", value: latestRun?.total_stocks || 0, color: "text-sky-400", glow: "shadow-sky-500/20" },
          { label: "Passed Filters", value: latestRun?.passed_filter || 0, color: "text-emerald-400", glow: "shadow-emerald-500/20" },
          { label: "Top Quality Pick", value: topPick?.ticker || "---", sub: `AM: ${topPick?.acquirers_multiple?.toFixed(1) || 0}x`, color: "text-indigo-400", glow: "shadow-indigo-500/20" },
          { label: "Clean M-Score", value: `${cleanCount} / ${rankings.length}`, color: "text-amber-400", glow: "shadow-amber-500/20" },
        ].map((stat) => (
          <motion.div 
            key={stat.label}
            variants={itemVariants}
            className="glass-card p-6 rounded-2xl hover:border-white/10 transition-all group relative overflow-hidden"
          >
            <div className={`absolute inset-0 bg-gradient-to-br from-transparent to-white/5 opacity-0 group-hover:opacity-100 transition-opacity`} />
            <p className="text-slate-500 text-[10px] font-black uppercase tracking-widest">{stat.label}</p>
            <p className={`text-4xl font-black mt-2 tracking-tighter ${stat.color}`}>{stat.value}</p>
            {stat.sub && <p className="text-slate-400 text-[10px] mt-1 font-mono uppercase tracking-tighter">{stat.sub}</p>}
          </motion.div>
        ))}
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-5 gap-8">
        {/* Advanced Distribution Chart */}
        <motion.div variants={itemVariants} className="lg:col-span-3 glass-card p-8 rounded-2xl flex flex-col">
          <div className="flex justify-between items-center mb-8">
            <h3 className="text-lg font-bold text-slate-100 tracking-tight flex items-center gap-2">
              Value vs Quality Distribution
              <span className="text-[10px] font-mono text-slate-500 bg-slate-800/50 px-2 py-0.5 rounded uppercase">Real-time</span>
            </h3>
          </div>
          <div className="h-[450px] w-full">
            <ResponsiveContainer width="100%" height="100%">
              <ScatterChart margin={{ top: 20, right: 20, bottom: 20, left: 0 }}>
                <CartesianGrid strokeDasharray="3 3" stroke="#1e293b" vertical={false} />
                <XAxis 
                  type="number" 
                  dataKey="am" 
                  name="AM" 
                  unit="x" 
                  stroke="#475569" 
                  fontSize={10}
                  tickLine={false}
                  axisLine={false}
                />
                <YAxis 
                  type="number" 
                  dataKey="quality" 
                  name="Quality" 
                  stroke="#475569" 
                  fontSize={10}
                  tickLine={false}
                  axisLine={false}
                />
                <ZAxis type="number" dataKey="mcap" range={[50, 800]} />
                <Tooltip content={<CustomTooltip />} />
                <Scatter name="Stocks" data={scatterData}>
                  {scatterData.map((entry, index) => (
                    <Cell 
                      key={`cell-${index}`} 
                      fill={entry.quality > 70 ? "#10b981" : entry.am < 10 ? "#0ea5e9" : "#64748b"} 
                      className="drop-shadow-[0_0_8px_rgba(56,189,248,0.3)] transition-all cursor-pointer hover:opacity-80"
                    />
                  ))}
                </Scatter>
              </ScatterChart>
            </ResponsiveContainer>
          </div>
        </motion.div>

        {/* High-Density Tables */}
        <motion.div variants={itemVariants} className="lg:col-span-2 glass-card rounded-2xl flex flex-col overflow-hidden">
          <div className="p-8 border-b border-white/5 flex justify-between items-center">
             <h3 className="text-lg font-bold text-slate-100 tracking-tight">Top Alpha Opportunities</h3>
             <span className="text-[10px] font-black text-sky-400 uppercase tracking-widest">N=10</span>
          </div>
          <div className="flex-1 overflow-y-auto custom-scrollbar px-2">
            <table className="w-full text-left border-collapse">
              <thead>
                <tr className="text-slate-500 text-[9px] uppercase tracking-[0.2em]">
                  <th className="px-6 py-4 font-black">Instrument</th>
                  <th className="px-4 py-4 font-black text-right">Mkt Cap</th>
                  <th className="px-4 py-4 font-black text-right">AM</th>
                  <th className="px-4 py-4 font-black text-right">Qual</th>
                  <th className="px-6 py-4 font-black text-right">M-Score</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-white/5">
                {rankings.slice(0, 10).map((item) => (
                  <tr key={item.ticker} className="group hover:bg-white/[0.02] transition-colors">
                    <td className="px-6 py-5">
                      <div className="font-black text-slate-100 text-lg tracking-tighter group-hover:text-sky-400 transition-colors uppercase">
                        {item.ticker}
                      </div>
                      <div className="text-[9px] text-slate-500 font-mono tracking-tighter uppercase">
                        Rank #{item.combined_rank}
                      </div>
                    </td>
                    <td className="px-4 py-5 text-right font-mono text-[10px] text-slate-400">{(item.market_cap_B || item.market_cap_b)?.toFixed(0)}B</td>
                    <td className="px-4 py-5 text-right font-mono text-xs text-slate-300">{item.acquirers_multiple?.toFixed(2)}x</td>
                    <td className="px-4 py-5 text-right font-mono text-xs text-slate-300">{item.quality_score?.toFixed(0)}</td>
                    <td className="px-6 py-5">
                      <div className="flex justify-end">
                        <BadgeDelta 
                          deltaType={item.beneish_mscore <= -1.78 ? "increase" : "decrease"} 
                          isIncreasePositive={true} 
                          size="xs"
                        >
                          <span className="font-mono text-[9px]">{item.beneish_mscore?.toFixed(2)}</span>
                        </BadgeDelta>
                      </div>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </motion.div>
      </div>
    </motion.div>
  );
}
