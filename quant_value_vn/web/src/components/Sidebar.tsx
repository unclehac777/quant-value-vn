"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";
import { ChartBarIcon, TableCellsIcon, StarIcon, Cog6ToothIcon } from "@heroicons/react/24/outline";

const navigation = [
  { name: "Dashboard", href: "/", icon: ChartBarIcon },
  { name: "Screener Results", href: "/screener", icon: TableCellsIcon },
  { name: "Watchlist & Portfolio", href: "/watchlist", icon: StarIcon },
];

export function Sidebar() {
  const pathname = usePathname();

  return (
    <div className="flex flex-col w-64 border-r border-gray-800 bg-gray-950 h-screen fixed top-0 left-0 p-4">
      <div className="flex items-center gap-2 px-2 py-4 mb-6">
        <span className="text-xl font-bold tracking-tight text-white">VN Quant Value</span>
      </div>
      
      <nav className="flex-1 space-y-1">
        {navigation.map((item) => {
          const isActive = pathname === item.href;
          return (
            <Link
              key={item.name}
              href={item.href}
              className={`
                group flex items-center px-2 py-2 text-sm font-medium rounded-md
                ${isActive 
                  ? "bg-gray-800 text-white" 
                  : "text-gray-400 hover:bg-gray-800 hover:text-white"
                }
              `}
            >
              <item.icon
                className={`mr-3 flex-shrink-0 h-5 w-5 ${isActive ? "text-gray-300" : "text-gray-400 group-hover:text-gray-300"}`}
                aria-hidden="true"
              />
              {item.name}
            </Link>
          );
        })}
      </nav>
      
      <div className="mt-auto">
        <Link
          href="/settings"
          className="group flex items-center px-2 py-2 text-sm font-medium rounded-md text-gray-400 hover:bg-gray-800 hover:text-white"
        >
          <Cog6ToothIcon className="mr-3 h-5 w-5 text-gray-400 group-hover:text-gray-300" />
          Settings
        </Link>
      </div>
    </div>
  );
}
