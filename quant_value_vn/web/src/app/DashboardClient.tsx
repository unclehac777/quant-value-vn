"use client";

import { Card, Metric, Text, Flex, Grid, BadgeDelta, Table, TableHead, TableRow, TableHeaderCell, TableBody, TableCell, ScatterChart } from "@tremor/react";

export default function DashboardClient({ rankings, runs }: { rankings: any[], runs: any[] }) {
  const latestRun = runs.length > 0 ? runs[0] : null;
  const topPick = rankings.length > 0 ? rankings[0] : null;

  // For the scatter chart
  const scatterData = rankings.map(r => ({
    "ticker": r.ticker,
    "Quality Score": r.quality_score,
    "Acquirers Multiple": r.acquirers_multiple,
    "Market Cap": r.market_cap_b,
  }));

  const cleanCount = rankings.filter(r => r.beneish_mscore <= -1.78).length;

  return (
    <div className="space-y-6">
      <Grid numItemsSm={2} numItemsLg={4} className="gap-6">
        <Card decoration="top" decorationColor="blue">
          <Text>Total Screened</Text>
          <Metric>{latestRun?.total_stocks || 0}</Metric>
        </Card>
        <Card decoration="top" decorationColor="emerald">
          <Text>Passed Filters</Text>
          <Metric>{latestRun?.passed_filter || 0}</Metric>
        </Card>
        <Card decoration="top" decorationColor="indigo">
          <Text>Top Pick</Text>
          <Metric>{topPick?.ticker || "---"}</Metric>
          <Text className="mt-2">AM: {topPick?.acquirers_multiple?.toFixed(1)}x</Text>
        </Card>
        <Card decoration="top" decorationColor="amber">
          <Text>Clean M-Score</Text>
          <Metric>{cleanCount} / {rankings.length}</Metric>
        </Card>
      </Grid>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        <Card>
          <Text>Value vs Quality Map</Text>
          <ScatterChart
            className="h-80 mt-6"
            yAxisWidth={50}
            data={scatterData}
            category="ticker"
            x="Acquirers Multiple"
            y="Quality Score"
            size="Market Cap"
            colors={["blue", "emerald", "amber", "rose", "indigo", "cyan"]}
          />
        </Card>

        <Card>
          <Flex alignItems="start">
            <Text>Top 10 Value Stocks</Text>
          </Flex>
          <Table className="mt-4 max-h-80 overflow-auto">
            <TableHead>
              <TableRow>
                <TableHeaderCell>Ticker</TableHeaderCell>
                <TableHeaderCell>Rank</TableHeaderCell>
                <TableHeaderCell className="text-right">AM (EV/EBIT)</TableHeaderCell>
                <TableHeaderCell className="text-right">Quality</TableHeaderCell>
                <TableHeaderCell className="text-right">M-Score</TableHeaderCell>
              </TableRow>
            </TableHead>
            <TableBody>
              {rankings.slice(0, 10).map((item) => (
                <TableRow key={item.ticker}>
                  <TableCell className="font-bold text-white">{item.ticker}</TableCell>
                  <TableCell>#{item.combined_rank}</TableCell>
                  <TableCell className="text-right">{item.acquirers_multiple?.toFixed(2)}x</TableCell>
                  <TableCell className="text-right">{item.quality_score?.toFixed(0)}</TableCell>
                  <TableCell className="text-right">
                    <BadgeDelta deltaType={item.beneish_mscore <= -1.78 ? "increase" : "decrease"} isIncreasePositive={true} size="xs">
                      {item.beneish_mscore?.toFixed(2)}
                    </BadgeDelta>
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </Card>
      </div>
    </div>
  );
}
