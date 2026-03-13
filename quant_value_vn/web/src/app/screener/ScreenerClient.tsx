"use client";

import { useState } from "react";
import { BadgeDelta } from "@tremor/react";
import { motion, AnimatePresence } from "framer-motion";
import { addToWatchlist, triggerScan } from "@/lib/api";
import { 
  PlusIcon, 
  CheckIcon, 
  ArrowPathIcon,
  ExclamationCircleIcon,
  MagnifyingGlassIcon,
  AdjustmentsHorizontalIcon,
  PlayIcon,
  ChevronUpIcon,
  ChevronDownIcon
} from "@heroicons/react/24/outline";

export default function ScreenerClient({ data }: { data: any[] }) {
  const [minQuality, setMinQuality] = useState<number>(0);
  const [maxAM, setMaxAM] = useState<number>(50);
  const [adding, setAdding] = useState<string | null>(null);
  const [added, setAdded] = useState<Set<string>>(new Set());
  const [scanning, setScanning] = useState(false);
  const [status, setStatus] = useState<{ type: 'track' | 'scan', msg: string, success: boolean } | null>(null);

  const [sortColumn, setSortColumn] = useState<string>("combined_rank");
  const [sortDirection, setSortDirection] = useState<'asc' | 'desc'>('asc');

  const filtered = data.filter((item) => {
    if (minQuality > 0 && item.quality_score < minQuality) return false;
    if (maxAM < 50 && item.acquirers_multiple > maxAM) return false;
    return true;
  });

  const handleSort = (column: string) => {
    if (sortColumn === column) {
      setSortDirection(prev => prev === 'asc' ? 'desc' : 'asc');
    } else {
      setSortColumn(column);
      setSortDirection('asc');
    }
  };

  const sortedData = [...filtered].sort((a, b) => {
    let valA = a[sortColumn];
    let valB = b[sortColumn];

    // Handle nulls
    if (valA === null || valA === undefined) return 1;
    if (valB === null || valB === undefined) return -1;

    if (typeof valA === 'string' && typeof valB === 'string') {
      valA = valA.toLowerCase();
      valB = valB.toLowerCase();
    }

    if (valA < valB) return sortDirection === 'asc' ? -1 : 1;
    if (valA > valB) return sortDirection === 'asc' ? 1 : -1;
    return 0;
  });

  const showToast = (type: 'track' | 'scan', msg: string, success: boolean = true) => {
    setStatus({ type, msg, success });
    setTimeout(() => setStatus(null), 3000);
  };

  const handleAdd = async (ticker: string) => {
    try {
      setAdding(ticker);
      await addToWatchlist(ticker);
      setAdded(prev => new Set(prev).add(ticker));
      showToast('track', `Added ${ticker} to watchlist`);
      setTimeout(() => setAdding(null), 1000);
    } catch (err: any) {
      const msg = err.response?.status === 409 ? `${ticker} already in watchlist` : `Failed to add ${ticker}`;
      showToast('track', msg, false);
      setAdding(null);
    }
  };

  const handleScan = async () => {
    try {
      setScanning(true);
      await triggerScan();
      showToast('scan', "Market scan initiated in background");
    } catch (err) {
      showToast('scan', "Failed to start scan", false);
    } finally {
      setScanning(false);
    }
  };

  const containerVariants = {
    hidden: { opacity: 0 },
    visible: { opacity: 1, transition: { staggerChildren: 0.02 } }
  };

  const rowVariants = {
    hidden: { opacity: 0, x: -10 },
    visible: { opacity: 1, x: 0 }
  };

  return (
    <div className="space-y-8 pb-12">
      <AnimatePresence>
        {status && (
          <motion.div 
            initial={{ opacity: 0, y: -20 }}
            animate={{ opacity: 1, y: 0 }}
            exit={{ opacity: 0, y: -20 }}
            className="fixed top-8 right-8 z-[100]"
          >
            <div className={`glass-panel px-6 py-4 rounded-2xl flex items-center gap-3 border shadow-2xl ${
              status.success ? (status.type === 'scan' ? 'border-sky-500/50' : 'border-emerald-500/50') : 'border-rose-500/50'
            }`}>
              {status.success ? (
                status.type === 'scan' ? <PlayIcon className="w-5 h-5 text-sky-400" /> : <CheckIcon className="w-5 h-5 text-emerald-400" />
              ) : (
                <ExclamationCircleIcon className="w-5 h-5 text-rose-400" />
              )}
              <span className="text-sm font-bold text-white">{status.msg}</span>
            </div>
          </motion.div>
        )}
      </AnimatePresence>

      {/* Modern High-End Filters */}
      <div className="grid grid-cols-1 lg:grid-cols-3 gap-6 items-end">
        <div className="lg:col-span-2 glass-card p-8 rounded-2xl">
          <div className="flex items-center justify-between mb-6">
            <div className="flex items-center gap-3">
              <AdjustmentsHorizontalIcon className="w-5 h-5 text-sky-400" />
              <h3 className="text-sm font-black text-slate-100 uppercase tracking-widest">Screener Parameters</h3>
            </div>
            
            <button 
              onClick={handleScan}
              disabled={scanning}
              className="flex items-center gap-2 px-4 py-2 bg-sky-500/10 border border-sky-400/20 rounded-xl text-[10px] font-black uppercase tracking-widest text-sky-400 hover:bg-sky-500 hover:text-white transition-all disabled:opacity-50 group"
            >
              {scanning ? (
                <ArrowPathIcon className="w-3.5 h-3.5 animate-spin" />
              ) : (
                <PlayIcon className="w-3.5 h-3.5 group-hover:fill-current" />
              )}
              {scanning ? "Scan in Progress..." : "Trigger New Scan"}
            </button>
          </div>
          <div className="grid grid-cols-1 sm:grid-cols-2 gap-8">
            <div className="space-y-3">
              <div className="flex justify-between">
                <label className="text-[10px] font-black text-slate-500 uppercase tracking-[0.2em]">Min Quality</label>
                <span className="text-xs font-mono text-emerald-400">{minQuality}</span>
              </div>
              <input 
                type="range"
                min="0"
                max="100"
                value={minQuality} 
                onChange={(e) => setMinQuality(Number(e.target.value))}
                className="w-full h-1.5 bg-slate-800 rounded-lg appearance-none cursor-pointer accent-sky-400"
              />
            </div>
            <div className="space-y-3">
              <div className="flex justify-between">
                <label className="text-[10px] font-black text-slate-500 uppercase tracking-[0.2em]">Max AM (EV/EBIT)</label>
                <span className="text-xs font-mono text-sky-400">{maxAM}x</span>
              </div>
              <input 
                type="range"
                min="0"
                max="50"
                step="0.5"
                value={maxAM} 
                onChange={(e) => setMaxAM(Number(e.target.value))}
                className="w-full h-1.5 bg-slate-800 rounded-lg appearance-none cursor-pointer accent-sky-400"
              />
            </div>
          </div>
        </div>

        <div className="glass-card p-8 rounded-2xl bg-sky-500/5 border-sky-500/10 h-full flex flex-col justify-center">
          <p className="text-slate-500 text-[10px] font-black uppercase tracking-widest">Instruments Identified</p>
          <div className="flex items-baseline gap-2 mt-4">
            <p className="text-5xl font-black text-white tracking-tighter">{filtered.length}</p>
            <span className="text-xs font-bold text-slate-500 uppercase tracking-widest">Assets</span>
          </div>
        </div>
      </div>

      {/* Bloomberg-style Results Table */}
      <div className="glass-card rounded-2xl overflow-hidden shadow-2xl">
        <div className="px-8 py-6 border-b border-white/5 flex justify-between items-center bg-white/[0.01]">
          <h3 className="text-lg font-bold text-slate-100 flex items-center gap-3 uppercase tracking-tighter">
            Market Scan Output
            <div className="flex items-center gap-1.5 px-2 py-0.5 bg-sky-400/10 rounded border border-sky-400/20">
               <div className="w-1 h-1 rounded-full bg-sky-400 animate-pulse" />
               <span className="text-[9px] font-black text-sky-400 uppercase tracking-widest">Live Alpha</span>
            </div>
          </h3>
        </div>
        
        <div className="overflow-x-auto custom-scrollbar">
          <motion.table 
            variants={containerVariants}
            initial="hidden"
            animate="visible"
            className="w-full text-left border-collapse min-w-[1000px]"
          >
            <thead>
              <tr className="text-slate-500 text-[9px] uppercase tracking-[0.2em] bg-white/[0.02] select-none">
                <SortableHeader label="Rank" column="combined_rank" currentSort={sortColumn} direction={sortDirection} onSort={handleSort} className="px-8" />
                <SortableHeader label="Instrument" column="ticker" currentSort={sortColumn} direction={sortDirection} onSort={handleSort} />
                <SortableHeader label="Mkt Cap" column="market_cap_b" currentSort={sortColumn} direction={sortDirection} onSort={handleSort} align="right" />
                <SortableHeader label="EV (B)" column="ev_b" currentSort={sortColumn} direction={sortDirection} onSort={handleSort} align="right" />
                <SortableHeader label="EBIT (B)" column="ebit" currentSort={sortColumn} direction={sortDirection} onSort={handleSort} align="right" />
                <SortableHeader label="Price" column="price" currentSort={sortColumn} direction={sortDirection} onSort={handleSort} align="right" />
                <SortableHeader label="AM Multiple" column="acquirers_multiple" currentSort={sortColumn} direction={sortDirection} onSort={handleSort} align="right" />
                <SortableHeader label="Yield %" column="ebit_ev" currentSort={sortColumn} direction={sortDirection} onSort={handleSort} align="right" />
                <SortableHeader label="Quality" column="quality_score" currentSort={sortColumn} direction={sortDirection} onSort={handleSort} align="right" />
                <SortableHeader label="M-Score" column="beneish_mscore" currentSort={sortColumn} direction={sortDirection} onSort={handleSort} align="right" />
                <SortableHeader label="P/B" column="pb" currentSort={sortColumn} direction={sortDirection} onSort={handleSort} align="right" />
                <th className="px-8 py-5 font-black text-right">Execution</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-white/5">
              {sortedData.map((item) => (
                <motion.tr 
                  variants={rowVariants}
                  key={item.ticker} 
                  className="group hover:bg-white/[0.03] transition-all"
                >
                  <td className="px-8 py-5">
                    <span className="text-slate-500 font-mono text-[10px] font-black tracking-widest">#{item.combined_rank}</span>
                  </td>
                  <td className="px-4 py-5">
                    <div className="font-black text-slate-100 text-xl tracking-tighter group-hover:text-sky-400 transition-colors uppercase cursor-default">
                      {item.ticker}
                    </div>
                    <div className="text-[9px] text-slate-500 font-bold uppercase tracking-[0.1em] mt-0.5">ASEAN REGION</div>
                  </td>
                  <td className="px-4 py-5 text-right">
                    <div className="text-slate-200 font-mono text-xs font-bold">
                      {(item.market_cap_b ?? (item.market_cap / 1e9))?.toLocaleString(undefined, { maximumFractionDigits: 1 })}B
                    </div>
                  </td>
                  <td className="px-4 py-5 text-right">
                    <div className="text-slate-200 font-mono text-xs font-bold">
                      {(item.ev_b ?? (item.enterprise_value / 1e9))?.toLocaleString(undefined, { maximumFractionDigits: 1 })}B
                    </div>
                  </td>
                  <td className="px-4 py-5 text-right">
                    <div className="text-slate-200 font-mono text-xs font-bold">
                      {(item.ebit / 1e9)?.toLocaleString(undefined, { maximumFractionDigits: 1 })}B
                    </div>
                  </td>
                  <td className="px-4 py-5 text-right">
                    <div className="text-slate-400 font-mono text-xs font-bold">{item.price?.toLocaleString()}</div>
                  </td>
                  <td className="px-4 py-5 text-right">
                    <div className="text-sky-400 font-mono text-sm font-black">{item.acquirers_multiple?.toFixed(2)}x</div>
                  </td>
                  <td className="px-4 py-5 text-right">
                    <div className="text-slate-300 font-mono text-xs font-bold">{(item.ebit_ev * 100)?.toFixed(1)}%</div>
                  </td>
                  <td className="px-4 py-5 text-right">
                     <div className="text-emerald-400 font-mono text-sm font-black">{item.quality_score?.toFixed(0)}</div>
                  </td>
                  <td className="px-4 py-5">
                    <div className="flex justify-end">
                      <BadgeDelta deltaType={item.beneish_mscore <= -1.78 ? "increase" : "decrease"} isIncreasePositive={true} size="xs">
                        <span className="font-mono text-[10px] font-bold">{item.beneish_mscore?.toFixed(2)}</span>
                      </BadgeDelta>
                    </div>
                  </td>
                  <td className="px-4 py-5 text-right text-slate-500 text-xs font-mono font-bold">{item.pb?.toFixed(2) || "---"}</td>
                  <td className="px-8 py-5 text-right">
                    <button 
                      onClick={() => handleAdd(item.ticker)}
                      disabled={added.has(item.ticker) || adding === item.ticker}
                      className={`
                        inline-flex items-center gap-2 px-4 py-2 rounded-xl text-[10px] font-black uppercase tracking-widest transition-all
                        ${added.has(item.ticker) 
                          ? "bg-emerald-500/10 text-emerald-400 border border-emerald-500/20 pointer-events-none shadow-[0_0_15px_rgba(16,185,129,0.1)]" 
                          : "bg-white/5 text-slate-400 border border-white/5 hover:bg-sky-500 hover:text-white hover:border-sky-400 shadow-xl active:scale-95 transition-all"
                        }
                        disabled:opacity-50
                      `}
                    >
                      {adding === item.ticker ? (
                        <ArrowPathIcon className="w-3.5 h-3.5 animate-spin" />
                      ) : added.has(item.ticker) ? (
                        <CheckIcon className="w-3.5 h-3.5 text-emerald-400" />
                      ) : (
                        <PlusIcon className="w-3.5 h-3.5" />
                      )}
                      <span className="hidden sm:inline">
                        {added.has(item.ticker) ? "Watched" : "Track"}
                      </span>
                    </button>
                  </td>
                </motion.tr>
              ))}
            </tbody>
          </motion.table>
        </div>
      </div>
    </div>
  );
}

function SortableHeader({ 
  label, 
  column, 
  currentSort, 
  direction, 
  onSort, 
  align = "left",
  className = "px-4"
}: { 
  label: string, 
  column: string, 
  currentSort: string, 
  direction: 'asc' | 'desc', 
  onSort: (col: string) => void,
  align?: "left" | "right",
  className?: string
}) {
  const isActive = currentSort === column;
  
  return (
    <th 
      className={`${className} py-5 font-black cursor-pointer group/th hover:text-white transition-colors`}
      onClick={() => onSort(column)}
    >
      <div className={`flex items-center gap-1.5 ${align === "right" ? "justify-end" : "justify-start"}`}>
        <span>{label}</span>
        <div className="flex flex-col -space-y-1 opacity-0 group-hover/th:opacity-50 transition-opacity">
          <ChevronUpIcon className={`w-2 h-2 ${isActive && direction === 'asc' ? 'text-sky-400 opacity-100' : ''}`} />
          <ChevronDownIcon className={`w-2 h-2 ${isActive && direction === 'desc' ? 'text-sky-400 opacity-100' : ''}`} />
        </div>
      </div>
    </th>
  );
}
