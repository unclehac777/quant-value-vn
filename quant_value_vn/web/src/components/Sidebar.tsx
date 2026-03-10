"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";
import { 
  ChartBarSquareIcon, 
  TableCellsIcon, 
  StarIcon, 
  Cog6ToothIcon 
} from "@heroicons/react/24/outline";

const navigation = [
  { name: "Market Overview", href: "/", icon: ChartBarSquareIcon },
  { name: "Screener", href: "/screener", icon: TableCellsIcon },
  { name: "Watchlist", href: "/watchlist", icon: StarIcon },
];

export default function Sidebar() {
  const pathname = usePathname();

  return (
    <div className="w-64 glass-panel h-screen fixed left-0 top-0 flex flex-col z-50">
      <div className="p-8">
        <div className="flex items-center gap-3">
          <div className="w-10 h-10 rounded-xl bg-sky-500 flex items-center justify-center shadow-[0_0_20px_rgba(14,165,233,0.3)]">
            <span className="text-white font-black text-xl">V</span>
          </div>
          <span className="text-xl font-black tracking-tighter text-slate-100 uppercase">Quant VN</span>
        </div>
      </div>

      <nav className="flex-1 px-4 space-y-2 mt-4">
        {navigation.map((item) => {
          const isActive = pathname === item.href;
          return (
            <Link
              key={item.name}
              href={item.href}
              className={`flex items-center gap-3 px-4 py-3 rounded-xl transition-all duration-300 group ${
                isActive 
                  ? "bg-sky-500/10 text-sky-400 border border-sky-500/20 shadow-[inset_0_0_10px_rgba(14,165,233,0.1)]" 
                  : "text-slate-400 hover:bg-slate-800/50 hover:text-slate-200"
              }`}
            >
              <item.icon className={`w-5 h-5 transition-colors ${isActive ? "text-sky-400" : "text-slate-500 group-hover:text-slate-300"}`} />
              <span className="text-sm font-semibold tracking-tight">{item.name}</span>
              {isActive && (
                <div className="ml-auto w-1.5 h-1.5 rounded-full bg-sky-400 shadow-[0_0_8px_rgba(14,165,233,0.6)]" />
              )}
            </Link>
          );
        })}
      </nav>

      <div className="p-4 border-t border-slate-800/50">
        <button className="flex items-center gap-3 px-4 py-3 w-full rounded-xl text-slate-500 hover:text-slate-300 hover:bg-slate-800/30 transition-all">
          <Cog6ToothIcon className="w-5 h-5" />
          <span className="text-sm font-semibold">Settings</span>
        </button>
      </div>
    </div>
  );
}
