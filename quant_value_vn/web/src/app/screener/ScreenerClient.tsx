"use client";

import { useState } from "react";
import { Card, Table, TableHead, TableRow, TableHeaderCell, TableBody, TableCell, BadgeDelta, TextInput, NumberInput } from "@tremor/react";

export default function ScreenerClient({ data }: { data: any[] }) {
  const [minQuality, setMinQuality] = useState<number>(0);
  const [maxAM, setMaxAM] = useState<number>(50);

  const filtered = data.filter((item) => {
    if (minQuality > 0 && item.quality_score < minQuality) return false;
    if (maxAM < 50 && item.acquirers_multiple > maxAM) return false;
    return true;
  });

  return (
    <div className="space-y-6">
      <Card>
        <div className="flex gap-4">
          <div className="flex-1">
            <label className="text-sm font-medium text-gray-300">Min. Quality Score</label>
            <NumberInput 
              value={minQuality} 
              onValueChange={setMinQuality} 
              className="mt-2"
              min={0}
              max={100}
            />
          </div>
          <div className="flex-1">
            <label className="text-sm font-medium text-gray-300">Max AM (EV/EBIT)</label>
            <NumberInput 
              value={maxAM} 
              onValueChange={setMaxAM} 
              className="mt-2"
              min={0}
              max={100}
              step={0.5}
            />
          </div>
        </div>
      </Card>

      <Card>
        <p className="text-sm text-gray-400 mb-4">Showing {filtered.length} / {data.length} stocks</p>
        <Table className="max-h-[600px] overflow-auto">
          <TableHead>
            <TableRow>
              <TableHeaderCell>Rank</TableHeaderCell>
              <TableHeaderCell>Ticker</TableHeaderCell>
              <TableHeaderCell className="text-right">AM (EV/EBIT)</TableHeaderCell>
              <TableHeaderCell className="text-right">EBIT/EV</TableHeaderCell>
              <TableHeaderCell className="text-right">Quality Score</TableHeaderCell>
              <TableHeaderCell className="text-right">M-Score</TableHeaderCell>
              <TableHeaderCell className="text-right">PE</TableHeaderCell>
              <TableHeaderCell className="text-right">PB</TableHeaderCell>
              <TableHeaderCell className="text-right">ROA</TableHeaderCell>
              <TableHeaderCell className="text-right">ROIC</TableHeaderCell>
            </TableRow>
          </TableHead>
          <TableBody>
            {filtered.map((item) => (
              <TableRow key={item.ticker}>
                <TableCell>#{item.combined_rank}</TableCell>
                <TableCell className="font-bold text-white">{item.ticker}</TableCell>
                <TableCell className="text-right">{item.acquirers_multiple?.toFixed(2)}x</TableCell>
                <TableCell className="text-right">{(item.ebit_ev * 100)?.toFixed(1)}%</TableCell>
                <TableCell className="text-right">{item.quality_score?.toFixed(0)}</TableCell>
                <TableCell className="text-right">
                  <BadgeDelta deltaType={item.beneish_mscore <= -1.78 ? "increase" : "decrease"} isIncreasePositive={true} size="xs">
                    {item.beneish_mscore?.toFixed(2)}
                  </BadgeDelta>
                </TableCell>
                <TableCell className="text-right">{item.pe?.toFixed(1) || "---"}</TableCell>
                <TableCell className="text-right">{item.pb?.toFixed(2) || "---"}</TableCell>
                <TableCell className="text-right">{(item.roa * 100)?.toFixed(1)}%</TableCell>
                <TableCell className="text-right">{(item.roic * 100)?.toFixed(1)}%</TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
      </Card>
    </div>
  );
}
