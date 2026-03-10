"use client";

import { useState } from "react";
import { 
  Card, Table, TableHead, TableRow, TableHeaderCell, TableBody, TableCell, 
  Button, TextInput, NumberInput, Grid, Metric, Text, BadgeDelta, Flex 
} from "@tremor/react";
import { removeFromWatchlist, updateWatchlistItem } from "@/lib/api";
import { TrashIcon, CheckIcon } from "@heroicons/react/24/outline";

export default function WatchlistClient({ initialWatchlist, rankings }: { initialWatchlist: any[], rankings: any[] }) {
  const [watchlist, setWatchlist] = useState(initialWatchlist);

  // Merge watchlist with current price data from rankings
  const enrichedWatchlist = watchlist.map(item => {
    const marketInfo = rankings.find(r => r.ticker === item.ticker);
    const currentPrice = marketInfo?.price || 0;
    const costBasis = (item.buy_price || 0) * (item.shares || 0);
    const currentValue = currentPrice * (item.shares || 0);
    const pnl = currentValue - costBasis;
    const pnlPercent = costBasis > 0 ? (pnl / costBasis) * 100 : 0;

    return {
      ...item,
      currentPrice,
      currentValue,
      costBasis,
      pnl,
      pnlPercent
    };
  });

  const totalCurrentValue = enrichedWatchlist.reduce((sum, item) => sum + item.currentValue, 0);
  const totalCostBasis = enrichedWatchlist.reduce((sum, item) => sum + item.costBasis, 0);
  const totalPnL = totalCurrentValue - totalCostBasis;
  const totalPnLPercent = totalCostBasis > 0 ? (totalPnL / totalCostBasis) * 100 : 0;

  const handleRemove = async (ticker: string) => {
    try {
      await removeFromWatchlist(ticker);
      setWatchlist(watchlist.filter(w => w.ticker !== ticker));
    } catch (err) {
      alert("Failed to remove item");
    }
  };

  const handleUpdate = async (ticker: string, data: any) => {
    try {
      await updateWatchlistItem(ticker, data);
      setWatchlist(watchlist.map(w => w.ticker === ticker ? { ...w, ...data } : w));
    } catch (err) {
      alert("Failed to update item");
    }
  };

  return (
    <div className="space-y-6">
      <Grid numItemsSm={1} numItemsLg={3} className="gap-6">
        <Card decoration="top" decorationColor="blue">
          <Text>Total Portfolio Value</Text>
          <Metric>{totalCurrentValue.toLocaleString()} VND</Metric>
        </Card>
        <Card decoration="top" decorationColor="slate">
          <Text>Total Cost Basis</Text>
          <Metric>{totalCostBasis.toLocaleString()} VND</Metric>
        </Card>
        <Card decoration="top" decorationColor={totalPnL >= 0 ? "emerald" : "rose"}>
          <Text>Total Profit / Loss</Text>
          <Flex justifyContent="start" alignItems="baseline" className="space-x-2">
            <Metric>{totalPnL.toLocaleString()} VND</Metric>
            <BadgeDelta deltaType={totalPnL >= 0 ? "moderateIncrease" : "moderateDecrease"}>
              {totalPnLPercent.toFixed(1)}%
            </BadgeDelta>
          </Flex>
        </Card>
      </Grid>

      <Card>
        <Table className="mt-4">
          <TableHead>
            <TableRow>
              <TableHeaderCell>Ticker</TableHeaderCell>
              <TableHeaderCell className="text-right">Price</TableHeaderCell>
              <TableHeaderCell className="text-right">Buy Price</TableHeaderCell>
              <TableHeaderCell className="text-right">Shares</TableHeaderCell>
              <TableHeaderCell className="text-right">Current Value</TableHeaderCell>
              <TableHeaderCell className="text-right">PnL</TableHeaderCell>
              <TableHeaderCell>Notes</TableHeaderCell>
              <TableHeaderCell className="text-right">Actions</TableHeaderCell>
            </TableRow>
          </TableHead>
          <TableBody>
            {enrichedWatchlist.map((item) => (
              <TableRow key={item.ticker}>
                <TableCell className="font-bold text-white">{item.ticker}</TableCell>
                <TableCell className="text-right">{item.currentPrice?.toLocaleString()}</TableCell>
                <TableCell className="text-right">
                  <div className="flex justify-end">
                    <NumberInput
                      className="w-24 h-8"
                      defaultValue={item.buy_price}
                      onValueChange={(val) => handleUpdate(item.ticker, { buy_price: val })}
                    />
                  </div>
                </TableCell>
                <TableCell className="text-right">
                   <div className="flex justify-end">
                    <NumberInput
                      className="w-24 h-8"
                      defaultValue={item.shares}
                      onValueChange={(val) => handleUpdate(item.ticker, { shares: val })}
                    />
                  </div>
                </TableCell>
                <TableCell className="text-right">{item.currentValue?.toLocaleString()}</TableCell>
                <TableCell className="text-right">
                  <BadgeDelta deltaType={item.pnl >= 0 ? "moderateIncrease" : "moderateDecrease"} size="xs">
                    {item.pnlPercent.toFixed(1)}%
                  </BadgeDelta>
                </TableCell>
                <TableCell>
                  <TextInput 
                    defaultValue={item.notes}
                    onBlur={(e) => handleUpdate(item.ticker, { notes: e.target.value })}
                  />
                </TableCell>
                <TableCell className="text-right">
                  <Button 
                    variant="light" 
                    color="rose" 
                    icon={TrashIcon} 
                    onClick={() => handleRemove(item.ticker)}
                  />
                </TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
      </Card>
    </div>
  );
}
