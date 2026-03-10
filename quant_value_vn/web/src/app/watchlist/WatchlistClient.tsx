"use client";

import { useState } from "react";
import { BadgeDelta } from "@tremor/react";
import { motion, AnimatePresence } from "framer-motion";
import { removeFromWatchlist, updateWatchlistItem } from "@/lib/api";
import { 
  TrashIcon, 
  ArrowPathIcon, 
  ExclamationCircleIcon,
  CheckCircleIcon
} from "@heroicons/react/24/outline";

export default function WatchlistClient({ initialWatchlist, rankings }: { initialWatchlist: any[], rankings: any[] }) {
  const [watchlist, setWatchlist] = useState(initialWatchlist);
  const [status, setStatus] = useState<{ type: 'success' | 'error' | 'loading', msg: string } | null>(null);

  // Merge watchlist with current price data from rankings
  const enrichedWatchlist = watchlist.map(item => {
    const marketInfo = rankings.find(r => r.ticker.toUpperCase() === item.ticker.toUpperCase());
    const currentPrice = marketInfo?.price || 0;
    const costBasis = (item.buy_price || 0) * (item.shares || 0);
    const currentValue = currentPrice * (item.shares || 0);
    const pnl = currentValue - costBasis;
    const pnlPercent = costBasis > 0 ? (pnl / costBasis) * 100 : 0;

    return { ...item, currentPrice, currentValue, costBasis, pnl, pnlPercent };
  });

  const totalCurrentValue = enrichedWatchlist.reduce((sum, item) => sum + item.currentValue, 0);
  const totalCostBasis = enrichedWatchlist.reduce((sum, item) => sum + item.costBasis, 0);
  const totalPnL = totalCurrentValue - totalCostBasis;
  const totalPnLPercent = totalCostBasis > 0 ? (totalPnL / totalCostBasis) * 100 : 0;

  const showToast = (type: 'success' | 'error' | 'loading', msg: string) => {
    setStatus({ type, msg });
    if (type !== 'loading') {
      setTimeout(() => setStatus(null), 3000);
    }
  };

  const handleRemove = async (ticker: string) => {
    try {
      showToast('loading', `Removing ${ticker}...`);
      await removeFromWatchlist(ticker);
      setWatchlist(watchlist.filter(w => w.ticker.toUpperCase() !== ticker.toUpperCase()));
      showToast('success', `${ticker} removed from watchlist`);
    } catch (err) {
      showToast('error', `Failed to remove ${ticker}`);
    }
  };

  const handleUpdate = async (ticker: string, data: any) => {
    try {
      await updateWatchlistItem(ticker, data);
      setWatchlist(watchlist.map(w => w.ticker.toUpperCase() === ticker.toUpperCase() ? { ...w, ...data } : w));
    } catch (err) {
      console.error("Update failed", err);
    }
  };

  return (
    <div className="space-y-8">
      {/* Toast Notification */}
      <AnimatePresence>
        {status && (
          <motion.div 
            initial={{ opacity: 0, y: -20 }}
            animate={{ opacity: 1, y: 0 }}
            exit={{ opacity: 0, y: -20 }}
            className="fixed top-8 right-8 z-[100]"
          >
            <div className={`glass-panel px-6 py-4 rounded-2xl flex items-center gap-3 border shadow-2xl ${
              status.type === 'success' ? 'border-emerald-500/50' : 
              status.type === 'error' ? 'border-rose-500/50' : 'border-sky-500/50'
            }`}>
              {status.type === 'loading' && <ArrowPathIcon className="w-5 h-5 text-sky-400 animate-spin" />}
              {status.type === 'success' && <CheckCircleIcon className="w-5 h-5 text-emerald-400" />}
              {status.type === 'error' && <ExclamationCircleIcon className="w-5 h-5 text-rose-400" />}
              <span className="text-sm font-bold text-white">{status.msg}</span>
            </div>
          </motion.div>
        )}
      </AnimatePresence>

      {/* Portfolio Totals Grid */}
      <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
        {[
          { label: "Equity Value", value: totalCurrentValue, color: "text-white" },
          { label: "Cost Basis", value: totalCostBasis, color: "text-slate-400" },
          { 
            label: "Total Unrealized PnL", 
            value: totalPnL, 
            percent: totalPnLPercent,
            color: totalPnL >= 0 ? "text-emerald-400" : "text-rose-400" 
          },
        ].map((stat) => (
          <div key={stat.label} className="glass-card p-8 rounded-2xl shadow-sm relative overflow-hidden group">
            <div className="absolute inset-0 bg-gradient-to-br from-transparent to-white/[0.02] pointer-events-none" />
            <p className="text-slate-500 text-[10px] font-black uppercase tracking-widest">{stat.label}</p>
            <div className="flex items-baseline gap-3 mt-3">
              <p className={`text-3xl font-black tracking-tighter font-mono ${stat.color}`}>{stat.value.toLocaleString()}</p>
              <span className="text-[10px] text-slate-600 font-bold uppercase">VND</span>
              {stat.percent !== undefined && (
                <div className={`ml-auto px-2 py-1 rounded-lg text-xs font-black ${
                  stat.percent >= 0 ? "bg-emerald-500/10 text-emerald-400" : "bg-rose-500/10 text-rose-400"
                }`}>
                  {stat.percent >= 0 ? "+" : ""}{stat.percent.toFixed(1)}%
                </div>
              )}
            </div>
          </div>
        ))}
      </div>

      {/* Holdings Table */}
      <div className="glass-card rounded-2xl overflow-hidden shadow-2xl">
        <div className="px-8 py-6 border-b border-white/5 bg-white/[0.01]">
          <h3 className="text-lg font-bold text-slate-100 flex items-center gap-3">
            Open Positions 
            <span className="text-[10px] font-mono text-slate-500 bg-slate-800/50 px-2 py-0.5 rounded uppercase">Active Tracking</span>
          </h3>
        </div>
        <div className="overflow-x-auto custom-scrollbar">
          <table className="w-full text-left border-collapse min-w-[1000px]">
            <thead>
              <tr className="text-slate-500 text-[9px] uppercase tracking-[0.2em] bg-white/[0.02]">
                <th className="px-8 py-5 font-black">Instrument</th>
                <th className="px-4 py-5 font-black text-right">Price</th>
                <th className="px-4 py-5 font-black text-center">Avg Cost</th>
                <th className="px-4 py-5 font-black text-center">Position</th>
                <th className="px-4 py-5 font-black text-right">Market Val</th>
                <th className="px-4 py-5 font-black text-right">Return</th>
                <th className="px-8 py-5 font-black text-right"></th>
              </tr>
            </thead>
            <tbody className="divide-y divide-white/5">
              {enrichedWatchlist.length === 0 && (
                <tr>
                  <td colSpan={7} className="px-8 py-24 text-center">
                    <p className="text-slate-500 text-sm font-semibold italic">Your portfolio is empty. Add instruments from the screener to begin tracking.</p>
                  </td>
                </tr>
              )}
              {enrichedWatchlist.map((item) => (
                <motion.tr 
                  layout
                  key={item.ticker} 
                  initial={{ opacity: 0 }}
                  animate={{ opacity: 1 }}
                  className="group hover:bg-white/[0.03] transition-all"
                >
                  <td className="px-8 py-5">
                    <div className="font-black text-slate-100 text-xl tracking-tighter group-hover:text-sky-400 transition-colors uppercase cursor-default">
                      {item.ticker}
                    </div>
                    <input 
                      type="text" 
                      defaultValue={item.notes}
                      onBlur={(e) => handleUpdate(item.ticker, { notes: e.target.value })}
                      placeholder="Add commentary..."
                      className="w-full bg-transparent border-none p-0 text-[10px] text-slate-500 focus:text-slate-300 focus:ring-0 focus:outline-none placeholder-slate-700 font-semibold"
                    />
                  </td>
                  <td className="px-4 py-5 text-right text-slate-300 font-mono text-sm">{item.currentPrice?.toLocaleString()}</td>
                  <td className="px-4 py-5 text-right">
                    <div className="flex justify-center">
                      <input 
                        type="number" 
                        defaultValue={item.buy_price}
                        onBlur={(e) => handleUpdate(item.ticker, { buy_price: Number(e.target.value) })}
                        className="w-20 bg-slate-950/50 border border-slate-800/50 rounded-lg px-2 py-1.5 text-right text-slate-100 font-mono text-xs focus:ring-1 focus:ring-sky-500/50 focus:border-sky-500/50 focus:outline-none transition-all"
                      />
                    </div>
                  </td>
                  <td className="px-4 py-5 text-right">
                    <div className="flex justify-center">
                      <input 
                        type="number" 
                        defaultValue={item.shares}
                        onBlur={(e) => handleUpdate(item.ticker, { shares: Number(e.target.value) })}
                        className="w-20 bg-slate-950/50 border border-slate-800/50 rounded-lg px-2 py-1.5 text-right text-slate-100 font-mono text-xs focus:ring-1 focus:ring-sky-500/50 focus:border-sky-500/50 focus:outline-none transition-all"
                      />
                    </div>
                  </td>
                  <td className="px-4 py-5 text-right text-white font-mono text-sm font-bold italic">{item.currentValue?.toLocaleString()}</td>
                  <td className="px-4 py-5 text-right">
                     <div className="flex justify-end">
                      <BadgeDelta deltaType={item.pnl >= 0 ? "moderateIncrease" : "moderateDecrease"} size="xs">
                        <span className="font-mono text-[10px] font-bold">{item.pnlPercent.toFixed(1)}%</span>
                      </BadgeDelta>
                    </div>
                  </td>
                  <td className="px-8 py-5 text-right">
                    <button 
                      onClick={() => handleRemove(item.ticker)}
                      title="Clear Position"
                      className="text-slate-600 hover:text-rose-500 transition-all p-2 rounded-lg hover:bg-rose-500/10 active:scale-90"
                    >
                      <TrashIcon className="w-4 h-4" />
                    </button>
                  </td>
                </motion.tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
}
