import { serve } from "https://deno.land/std@0.168.0/http/server.ts";

const corsHeaders = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers": "authorization, x-client-info, apikey, content-type",
};

const FH_KEY = "d7hji19r01qhiu0brkigd7hji19r01qhiu0brkj0";

// ── Yahoo Finance fetch helper ─────────────────────────────────────────────────
async function yahooFetch(url: string, timeout = 12000): Promise<any> {
  for (const host of ["query1", "query2"]) {
    const u = url.replace("query1", host);
    try {
      const ctrl = new AbortController();
      const t = setTimeout(() => ctrl.abort(), timeout);
      const res = await fetch(u, {
        signal: ctrl.signal,
        headers: {
          "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
          "Accept": "application/json",
          "Origin": "https://finance.yahoo.com",
          "Referer": "https://finance.yahoo.com/",
        },
      });
      clearTimeout(t);
      if (res.ok) return await res.json();
      console.log(`Yahoo ${host} HTTP ${res.status} for ${u.slice(0,80)}`);
    } catch(e: any) { console.log(`Yahoo ${host} error: ${e.message}`); }
  }
  return null;
}

// ── FX RATES via Yahoo Finance v7 quote ───────────────────────────────────────
async function fetchFxRates(): Promise<Record<string, number>> {
  const defaults: Record<string,number> = {
    USD:1.27, JPY:0.0080, EUR:1.49, HKD:0.163, GBP:1.68, AUD:0.81, CNY:0.175, TWD:0.039, SGD:1.0
  };

  const pairs = ["USDSGD=X","JPYSGD=X","EURSGD=X","HKDSGD=X","GBPSGD=X","AUDSGD=X","CNYSGD=X","TWDSGD=X"];
  const ccyMap: Record<string,string> = {
    "USDSGD=X":"USD","JPYSGD=X":"JPY","EURSGD=X":"EUR","HKDSGD=X":"HKD",
    "GBPSGD=X":"GBP","AUDSGD=X":"AUD","CNYSGD=X":"CNY","TWDSGD=X":"TWD"
  };
  const rates: Record<string,number> = { SGD: 1.0 };

  // Fetch each pair individually via Yahoo v8 chart (more reliable than v7 quote for FX)
  await Promise.allSettled(pairs.map(async (pair) => {
    const url = `https://query1.finance.yahoo.com/v8/finance/chart/${encodeURIComponent(pair)}?interval=1d&range=1d`;
    try {
      const d = await yahooFetch(url, 8000);
      const closes = d?.chart?.result?.[0]?.indicators?.quote?.[0]?.close;
      const meta   = d?.chart?.result?.[0]?.meta;
      const price  = meta?.regularMarketPrice || (closes && closes[closes.length-1]);
      if (price && price > 0) {
        const ccy = ccyMap[pair];
        if (ccy) rates[ccy] = parseFloat(price.toFixed(6));
      }
    } catch(e: any) { console.log(`[fx] ${pair} error: ${e.message}`); }
  }));

  const count = Object.keys(rates).length;
  console.log(`[fx_rates] Got ${count} rates:`, Object.entries(rates).map(([k,v])=>`${k}=${v}`).join(", "));
  return count > 1 ? rates : defaults;
}

// ── STOCK PRICES via Finnhub ──────────────────────────────────────────────────
// ── YAHOO FINANCE MULTI-EXCHANGE TICKER MAPPING ─────────────────────────────
// Map app tickers to Yahoo-format tickers for various exchanges
function yahooTicker(ticker: string, mkt?: string): string {
  // Already has a suffix? Use as-is
  if (ticker.includes(".") || ticker.includes("-")) return ticker;
  // Apply market-specific Yahoo suffix
  switch((mkt||"").toUpperCase()) {
    case "SG": return ticker + ".SI";   // Singapore
    case "HK":
    case "CN": return ticker + ".HK";   // Hong Kong
    case "JP": return ticker + ".T";    // Tokyo
    case "GB": return ticker + ".L";    // London
    case "EU": return ticker + ".PA";   // Paris
    case "DE": return ticker + ".DE";   // Frankfurt
    case "AU": return ticker + ".AX";   // Australia
    case "TW": return ticker + ".TW";   // Taiwan
    default: return ticker;              // US default
  }
}

// ── YAHOO FINANCE PRICE FETCH (primary — no rate limits) ─────────────────────
async function yahooPrices(
  tickers: string[],
  tickerMktMap?: Record<string,string>
): Promise<Record<string,number>> {
  const results: Record<string,number> = {};
  const concurrency = 10; // fetch 10 at a time to be polite
  async function one(ticker: string) {
    const yt = yahooTicker(ticker, tickerMktMap?.[ticker]);
    const url = `https://query1.finance.yahoo.com/v8/finance/chart/${encodeURIComponent(yt)}?interval=1d&range=5d`;
    try {
      const ctrl = new AbortController();
      const t = setTimeout(() => ctrl.abort(), 6000);
      const res = await fetch(url, {
        signal: ctrl.signal,
        headers: { "User-Agent": "Mozilla/5.0", "Accept": "application/json" }
      });
      clearTimeout(t);
      if (!res.ok) return;
      const d = await res.json();
      const meta = d?.chart?.result?.[0]?.meta;
      const price = meta?.regularMarketPrice || meta?.previousClose || 0;
      if (price > 0) results[ticker] = parseFloat(price.toFixed(4));
    } catch(e: any) { /* silent - try Finnhub fallback */ }
  }
  // Chunked parallel fetching
  for (let i = 0; i < tickers.length; i += concurrency) {
    const chunk = tickers.slice(i, i + concurrency);
    await Promise.allSettled(chunk.map(one));
  }
  return results;
}

// ── FINNHUB FALLBACK (secondary — for tickers Yahoo couldn't resolve) ───────
async function finnhubPrices(tickers: string[]): Promise<Record<string,number>> {
  const results: Record<string,number> = {};
  async function one(ticker: string) {
    const url = `https://finnhub.io/api/v1/quote?symbol=${encodeURIComponent(ticker)}&token=${FH_KEY}`;
    try {
      const ctrl = new AbortController();
      const t = setTimeout(() => ctrl.abort(), 6000);
      const res = await fetch(url, { signal: ctrl.signal });
      clearTimeout(t);
      if (!res.ok) return;
      const d = await res.json();
      const p = d.c || d.pc;
      if (p && p > 0) results[ticker] = parseFloat(p.toFixed(4));
    } catch(e: any) { /* silent */ }
  }
  await Promise.allSettled(tickers.map(one));
  return results;
}

// ── UNIFIED PRICE FETCH — Yahoo primary, Finnhub fallback ───────────────────
async function getPrices(
  tickers: string[],
  tickerMktMap?: Record<string,string>
): Promise<Record<string,number>> {
  // Try Yahoo first for everyone
  console.log(`[getPrices] Yahoo Finance attempt: ${tickers.length} tickers`);
  const yahooResults = await yahooPrices(tickers, tickerMktMap);
  const yahooCount = Object.keys(yahooResults).length;
  console.log(`[getPrices] Yahoo returned ${yahooCount}/${tickers.length}`);

  // For tickers Yahoo missed, fall back to Finnhub (US-only)
  const missing = tickers.filter(t => !yahooResults[t]);
  if (missing.length > 0) {
    console.log(`[getPrices] Finnhub fallback for ${missing.length} tickers`);
    const fhResults = await finnhubPrices(missing);
    const fhCount = Object.keys(fhResults).length;
    console.log(`[getPrices] Finnhub returned ${fhCount}/${missing.length}`);
    Object.assign(yahooResults, fhResults);
  }

  return yahooResults;
}

// ── HISTORICAL CANDLES via Yahoo ──────────────────────────────────────────────
async function yahooHistory(ticker: string, period: string): Promise<number[]> {
  const rangeMap:    Record<string,string> = {"30d":"1mo","6m":"6mo","1y":"1y","5y":"5y","all":"10y"};
  const intervalMap: Record<string,string> = {"30d":"1d","6m":"1d","1y":"1d","5y":"1wk","all":"1wk"};
  const url = `https://query1.finance.yahoo.com/v8/finance/chart/${encodeURIComponent(ticker)}?interval=${intervalMap[period]||"1d"}&range=${rangeMap[period]||"6mo"}`;
  try {
    const d      = await yahooFetch(url);
    const closes = d?.chart?.result?.[0]?.indicators?.quote?.[0]?.close;
    if (!closes?.length) return [];
    return closes.filter((v: any) => v != null && !isNaN(v)).map((v: number) => parseFloat(v.toFixed(4)));
  } catch(e: any) { console.log(`History error ${ticker}: ${e.message}`); return []; }
}

// ── MAIN HANDLER ──────────────────────────────────────────────────────────────
serve(async (req) => {
  if (req.method === "OPTIONS") return new Response("ok", { headers: corsHeaders });

  try {
    const body   = await req.json();
    const action = body.action || "prices";

    if (action === "fx_rates") {
      const rates = await fetchFxRates();
      return new Response(JSON.stringify({ rates }),
        { headers: { ...corsHeaders, "Content-Type": "application/json" } });
    }

    if (action === "dividends") {
      // Dividend yield retrieval — market-specific strategy:
      // SG/HK/JP/EU: sum 1yr dividends from Yahoo chart events (most reliable for non-US)
      // US: Finnhub dividendYieldIndicatedAnnual (most reliable for US)
      // Fallback: Yahoo v7/quote trailingAnnualDividendYield for any missed
      const { tickers: divTickers, holdings: divHoldings } = body;
      if (!Array.isArray(divTickers) || !divTickers.length) {
        return new Response(JSON.stringify({ divYields: {} }),
          { headers: { ...corsHeaders, "Content-Type": "application/json" } });
      }

      const tickerMktMap2: Record<string,string> = {};
      if (Array.isArray(divHoldings)) {
        divHoldings.forEach((h: any) => {
          if (h?.ticker && h?.mkt) tickerMktMap2[h.ticker] = h.mkt;
        });
      }

      const divYields: Record<string,number> = {};
      const oneYearAgo = Math.floor(Date.now() / 1000) - 365 * 24 * 3600;

      // STRATEGY 1: Yahoo chart API with dividend events (works well for SG, HK, JP, EU)
      const nonUsTickers = divTickers.filter(t => {
        const mkt = tickerMktMap2[t] || "US";
        return mkt !== "US";
      });

      console.log(`[dividends] Non-US tickers (chart events method): ${nonUsTickers.length}`);
      const concurrency = 8;
      for (let i = 0; i < nonUsTickers.length; i += concurrency) {
        const batch = nonUsTickers.slice(i, i + concurrency);
        await Promise.allSettled(batch.map(async (t: string) => {
          try {
            const mkt = tickerMktMap2[t] || "";
            const yt = yahooTicker(t, mkt);
            // Fetch 1 year of daily data with dividend events
            const url = `https://query2.finance.yahoo.com/v8/finance/chart/${encodeURIComponent(yt)}?range=1y&interval=1d&events=dividends`;
            const ctrl = new AbortController();
            const tm = setTimeout(() => ctrl.abort(), 10000);
            const res = await fetch(url, {
              signal: ctrl.signal,
              headers: {
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
                "Accept": "application/json",
                "Referer": "https://finance.yahoo.com/",
              }
            });
            clearTimeout(tm);
            if (!res.ok) return;
            const d = await res.json();
            const result = d?.chart?.result?.[0];
            if (!result) return;

            const currentPrice = result.meta?.regularMarketPrice || result.meta?.previousClose || 0;
            const dividendEvents = result.events?.dividends || {};

            // Sum all dividend amounts paid in the last 12 months
            let annualDivSum = 0;
            Object.values(dividendEvents).forEach((ev: any) => {
              if (ev?.date >= oneYearAgo && ev?.amount > 0) {
                annualDivSum += ev.amount;
              }
            });

            if (annualDivSum > 0 && currentPrice > 0) {
              const yieldPct = parseFloat((annualDivSum / currentPrice * 100).toFixed(4));
              divYields[t] = yieldPct;
              console.log(`[dividends] ${t} (${yt}): $${annualDivSum.toFixed(3)} / $${currentPrice} = ${yieldPct.toFixed(2)}%`);
            } else {
              // Fallback: try trailingAnnualDividendYield from meta
              const meta = result.meta || {};
              const rawYield = (meta as any).trailingAnnualDividendYield || 0;
              const rawRate  = (meta as any).trailingAnnualDividendRate  || 0;
              if (rawYield > 0) {
                divYields[t] = parseFloat((rawYield * 100).toFixed(4));
              } else if (rawRate > 0 && currentPrice > 0) {
                divYields[t] = parseFloat((rawRate / currentPrice * 100).toFixed(4));
              }
            }
          } catch(e2: any) {
            console.log(`[dividends] chart error for ${t}: ${e2.message}`);
          }
        }));
      }

      // STRATEGY 2: Finnhub for US stocks — dividend yield + PE ratio in one call (FIX 1)
      // Zero extra API cost: PE piggybacks on the same Finnhub metric call
      const peRatios: Record<string,number> = {};
      const usTickers = divTickers.filter(t => {
        const mkt = tickerMktMap2[t] || "US";
        return mkt === "US";
      });
      console.log(`[dividends] US tickers (Finnhub div+PE): ${usTickers.length}`);
      for (let i = 0; i < usTickers.length; i += 8) {
        const batch = usTickers.slice(i, i + 8);
        await Promise.allSettled(batch.map(async (t: string) => {
          try {
            const r = await fetch(
              `https://finnhub.io/api/v1/stock/metric?symbol=${encodeURIComponent(t)}&metric=all&token=${FH_KEY}`,
              { headers: { "User-Agent": "Mozilla/5.0" } }
            );
            if (!r.ok) return;
            const m = await r.json();
            // Dividend yield
            const dy = m?.metric?.dividendYieldIndicatedAnnual
                    || m?.metric?.currentDividendYieldTTM
                    || 0;
            if (dy > 0) divYields[t] = parseFloat(dy.toFixed(4));
            // PE ratio — FIX 1: captures live PE to fix permanently-zero PE in Buffett score
            const pe = m?.metric?.peBasicExclExtraTTM || m?.metric?.peTTM || 0;
            if (pe > 0 && pe < 2000) peRatios[t] = parseFloat(pe.toFixed(2));
          } catch { /* silent */ }
        }));
        if (i + 8 < usTickers.length) {
          await new Promise(resolve => setTimeout(resolve, 1100));
        }
      }

      // STRATEGY 3: Yahoo v7/quote bulk for any still missing
      const stillMissing = divTickers.filter(t => !divYields[t]);
      if (stillMissing.length > 0) {
        console.log(`[dividends] Yahoo v7/quote fallback for ${stillMissing.length} tickers`);
        const yahooToOrig: Record<string,string> = {};
        const yahooSyms = stillMissing.map(t => {
          const yt = yahooTicker(t, tickerMktMap2[t] || "US");
          yahooToOrig[yt] = t;
          yahooToOrig[t] = t;
          return yt;
        });
        try {
          const url = `https://query2.finance.yahoo.com/v7/finance/quote?symbols=${encodeURIComponent(yahooSyms.join(","))}`;
          const res = await fetch(url, {
            headers: {
              "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
              "Accept": "application/json",
              "Referer": "https://finance.yahoo.com/",
            }
          });
          if (res.ok) {
            const d = await res.json();
            (d?.quoteResponse?.result || []).forEach((q: any) => {
              const sym = q?.symbol || "";
              const orig = yahooToOrig[sym] || yahooToOrig[sym.toUpperCase()] || sym;
              const rawYield = q?.trailingAnnualDividendYield || 0;
              const rawRate  = q?.trailingAnnualDividendRate  || 0;
              const price    = q?.regularMarketPrice || 0;
              if (rawYield > 0) divYields[orig] = parseFloat((rawYield * 100).toFixed(4));
              else if (rawRate > 0 && price > 0) divYields[orig] = parseFloat((rawRate / price * 100).toFixed(4));
            });
          }
        } catch(e3: any) { console.log(`[dividends] v7/quote fallback error: ${e3.message}`); }
      }

      console.log(`[dividends] Final: ${Object.keys(divYields).length}/${divTickers.length} yields, ${Object.keys(peRatios).length} PE ratios`);
      return new Response(JSON.stringify({ divYields, peRatios }),
        { headers: { ...corsHeaders, "Content-Type": "application/json" } });
    }

    
    if (action === "prices") {
      const { tickers, holdings } = body;
      if (!Array.isArray(tickers) || !tickers.length)
        return new Response(JSON.stringify({ error: "tickers required" }),
          { status: 400, headers: { ...corsHeaders, "Content-Type": "application/json" } });

      // Build ticker→market map for Yahoo suffix selection
      const tickerMktMap: Record<string,string> = {};
      if (Array.isArray(holdings)) {
        holdings.forEach((h: any) => {
          if (h?.ticker && h?.mkt) tickerMktMap[h.ticker] = h.mkt;
        });
      }

      console.log(`[prices] ${tickers.length} tickers, ${Object.keys(tickerMktMap).length} with market info`);
      const prices = await getPrices(tickers, tickerMktMap);
      console.log(`[prices] Total returned: ${Object.keys(prices).length}/${tickers.length}`);
      return new Response(JSON.stringify({ prices, count: Object.keys(prices).length }),
        { headers: { ...corsHeaders, "Content-Type": "application/json" } });
    }

    if (action === "history") {
      const { ticker, period } = body;
      if (!ticker) return new Response(JSON.stringify({ error: "ticker required" }),
        { status: 400, headers: { ...corsHeaders, "Content-Type": "application/json" } });
      console.log(`[history] ${ticker} ${period}`);
      const closes = await yahooHistory(ticker, period || "6m");
      console.log(`[history] ${closes.length} candles for ${ticker}`);
      return new Response(JSON.stringify({ ticker, period, closes, count: closes.length }),
        { headers: { ...corsHeaders, "Content-Type": "application/json" } });
    }

    if (action === "portfolio_chart") {
      const { indexTicker, holdingTickers, period } = body;
      if (!indexTicker) return new Response(JSON.stringify({ error: "indexTicker required" }),
        { status: 400, headers: { ...corsHeaders, "Content-Type": "application/json" } });
      console.log(`[portfolio_chart] index=${indexTicker} holdings=${holdingTickers?.length} period=${period}`);
      const allTickers = [indexTicker, ...(holdingTickers || [])];
      const histMap: Record<string, number[]> = {};
      await Promise.allSettled(allTickers.map(async (t: string) => {
        const c = await yahooHistory(t, period || "6m");
        if (c.length > 1) histMap[t] = c;
      }));
      const indexCloses = histMap[indexTicker] || [];
      const holdingHistories: Record<string,number[]> = {};
      (holdingTickers || []).forEach((t: string) => { if (histMap[t]) holdingHistories[t] = histMap[t]; });
      console.log(`[portfolio_chart] index=${indexCloses.length}pts holdings=${Object.keys(holdingHistories).length}/${holdingTickers?.length}`);
      return new Response(JSON.stringify({ indexCloses, holdingHistories, indexTicker, period }),
        { headers: { ...corsHeaders, "Content-Type": "application/json" } });
    }

    // ── SENATE TRADES ─────────────────────────────────────────────────────────
    if (action === "senate_trades") {
      // Quiver Quantitative congressional trading API
      const QQ_TOKEN = "6785fd1cc434bf4d026c0f700caf903d8ab30f7a";
      const url = "https://api.quiverquant.com/beta/live/congresstrading";

      // Retry wrapper
      async function tryFetch(attempt = 1): Promise<any[]> {
        try {
          const ctrl = new AbortController();
          const t = setTimeout(() => ctrl.abort(), 15000);
          const res = await fetch(url, {
            signal: ctrl.signal,
            headers: {
              "Accept": "application/json",
              "Authorization": `Token ${QQ_TOKEN}`,
              "X-CSRFToken": "quiverquant",
              "User-Agent": "Ignitus/1.0",
            },
          });
          clearTimeout(t);
          console.log(`[senate_trades] Attempt ${attempt} status: ${res.status}`);
          if (!res.ok) {
            const errText = (await res.text()).slice(0, 300);
            console.error(`[senate_trades] Attempt ${attempt} error: ${errText}`);
            if (attempt < 2 && (res.status === 429 || res.status >= 500)) {
              await new Promise(r => setTimeout(r, 2000));
              return tryFetch(attempt + 1);
            }
            throw new Error(`HTTP ${res.status}`);
          }
          const all = await res.json();
          if (!Array.isArray(all)) throw new Error("Non-array response");
          return all;
        } catch(e: any) {
          if (attempt < 2) {
            console.log(`[senate_trades] Retrying after error: ${e.message}`);
            await new Promise(r => setTimeout(r, 2000));
            return tryFetch(attempt + 1);
          }
          throw e;
        }
      }

      try {
        console.log("[senate_trades] Fetching from Quiver Quantitative");
        const all = await tryFetch();
        console.log(`[senate_trades] Quiver returned ${all.length} total records`);

        if (all.length === 0) {
          return new Response(
            JSON.stringify({ trades: [], error: "Quiver returned empty list" }),
            { headers: { ...corsHeaders, "Content-Type": "application/json" } }
          );
        }

        // Debug: log field names of first record
        console.log("[senate_trades] First record fields:", Object.keys(all[0]).join(", "));

        // Step 1: Get only Senate chamber records (preferred)
        let filtered = all.filter((t: any) => {
          const chamber = (t.Chamber || t.chamber || "").toString();
          const ticker = (t.Ticker || t.ticker || "").toString().trim();
          const txn = (t.Transaction || t.transaction || t.Type || "").toString();
          return chamber === "Senate" &&
            ticker && ticker !== "--" && ticker.length <= 6 &&
            (txn.includes("Purchase") || txn.includes("Sale") || txn.includes("Buy") || txn.includes("Sell"));
        });
        console.log(`[senate_trades] Senate filter: ${filtered.length}`);

        // Step 2: If no Senate-only records, try including House too (user sees SOMETHING)
        if (filtered.length === 0) {
          filtered = all.filter((t: any) => {
            const ticker = (t.Ticker || t.ticker || "").toString().trim();
            const txn = (t.Transaction || t.transaction || t.Type || "").toString();
            return ticker && ticker !== "--" && ticker.length <= 6 &&
              (txn.includes("Purchase") || txn.includes("Sale") || txn.includes("Buy") || txn.includes("Sell"));
          });
          console.log(`[senate_trades] Expanded to all congress: ${filtered.length}`);
        }

        // Sort by most recent, take top 10
        const sorted = filtered
          .sort((a: any, b: any) => {
            const da = new Date(a.TransactionDate || a.transaction_date || a.Date || 0);
            const db = new Date(b.TransactionDate || b.transaction_date || b.Date || 0);
            return db.getTime() - da.getTime();
          })
          .slice(0, 10)
          .map((t: any) => {
            const rawAmt = t.Amount || t.amount || 0;
            let amtStr = "N/A";
            if (typeof rawAmt === "string" && rawAmt.includes("$")) {
              amtStr = rawAmt;
            } else {
              const n = parseFloat(rawAmt) || 0;
              if (n >= 1000001) amtStr = "$1,000,001+";
              else if (n >= 500001) amtStr = "$500,001 - $1,000,000";
              else if (n >= 250001) amtStr = "$250,001 - $500,000";
              else if (n >= 100001) amtStr = "$100,001 - $250,000";
              else if (n >= 50001)  amtStr = "$50,001 - $100,000";
              else if (n >= 15001)  amtStr = "$15,001 - $50,000";
              else if (n >= 1001)   amtStr = "$1,001 - $15,000";
              else if (n > 0)       amtStr = `$${n.toLocaleString()}`;
            }
            return {
              name:   t.Representative || t.representative || t.Senator || "Unknown",
              ticker: (t.Ticker || t.ticker || "").trim().toUpperCase(),
              action: (t.Transaction || t.transaction || "").includes("Sale") ? "SELL" : "BUY",
              amount: amtStr,
              date:   t.TransactionDate || t.transaction_date || t.Date || "",
              sector: t.Sector || t.sector || t.Industry || "",
              source: "Quiver Quant / STOCK Act",
              party:  t.Party || t.party || "?",
            };
          });

        console.log(`[senate_trades] Returning ${sorted.length} trades`);
        return new Response(JSON.stringify({ trades: sorted, total: all.length }),
          { headers: { ...corsHeaders, "Content-Type": "application/json" } });

      } catch(e: any) {
        console.error(`[senate_trades] Final failure: ${e.message}`);
        return new Response(JSON.stringify({ trades: [], error: e.message }),
          { headers: { ...corsHeaders, "Content-Type": "application/json" } });
      }
    }

    if (action === "senate_prices") {
      // Fetch live price + basic metrics for senate tickers not in portfolio
      // Used to compute Graham Number intrinsic value
      const FH_KEY = "d7hji19r01qhiu0brkigd7hji19r01qhiu0brkj0";
      const tickers: string[] = body.tickers || [];
      if (tickers.length === 0) {
        return new Response(JSON.stringify({ prices: [] }),
          { headers: { ...corsHeaders, "Content-Type": "application/json" } });
      }

      const results = await Promise.allSettled(tickers.map(async (ticker) => {
        try {
          // Fetch quote + metrics in parallel
          const [qRes, mRes] = await Promise.all([
            fetch(`https://finnhub.io/api/v1/quote?symbol=${ticker}&token=${FH_KEY}`),
            fetch(`https://finnhub.io/api/v1/stock/metric?symbol=${ticker}&metric=all&token=${FH_KEY}`)
          ]);

          const q = qRes.ok ? await qRes.json() : {};
          const m = mRes.ok ? await mRes.json() : {};

          const price = q.c || q.pc || 0;
          const metric = m.metric || {};

          // Graham Number = sqrt(22.5 × EPS × BVPS)
          const eps  = metric.epsBasicExclExtraItemsTTM || metric.epsTTM || 0;
          const bvps = metric.bookValuePerShareAnnual   || metric.bvps   || 0;
          const pe   = metric.peBasicExclExtraTTM       || metric.peTTM  || 0;
          const div  = metric.dividendYieldIndicatedAnnual || 0;

          let intrinsic = 0;
          if (eps > 0 && bvps > 0) {
            intrinsic = parseFloat(Math.sqrt(22.5 * eps * bvps).toFixed(2));
          } else if (price > 0 && pe > 0 && pe < 50) {
            // Fallback: fair PE estimate (15x)
            intrinsic = parseFloat((eps * 15).toFixed(2));
          }

          return { ticker, price, intrinsic, pe, eps, bvps, div };
        } catch(e) {
          return { ticker, price: 0, intrinsic: 0, pe: 0, eps: 0, bvps: 0, div: 0 };
        }
      }));

      const prices = results
        .filter(r => r.status === "fulfilled")
        .map(r => (r as PromiseFulfilledResult<any>).value)
        .filter(r => r.price > 0);

      console.log(`[senate_prices] Fetched ${prices.length}/${tickers.length} prices`);
      return new Response(JSON.stringify({ prices }),
        { headers: { ...corsHeaders, "Content-Type": "application/json" } });
    }


    if (action === "senate_history") {
      // Fetch historical stock price on a specific date for senate trades
      const ticker: string = body.ticker || "";
      const date: string = body.date || ""; // YYYY-MM-DD
      if (!ticker || !date) {
        return new Response(JSON.stringify({ price: 0 }),
          { headers: { ...corsHeaders, "Content-Type": "application/json" } });
      }

      try {
        // Convert date to Unix timestamps
        const d = new Date(date);
        const from = Math.floor(d.getTime() / 1000) - 86400; // day before
        const to   = Math.floor(d.getTime() / 1000) + 86400; // day after
        const url = `https://query1.finance.yahoo.com/v8/finance/chart/${encodeURIComponent(ticker)}?interval=1d&period1=${from}&period2=${to}`;
        
        const res = await fetch(url, {
          headers: { "User-Agent": "Mozilla/5.0", "Accept": "application/json" }
        });
        if (!res.ok) throw new Error(`Yahoo ${res.status}`);
        const d2 = await res.json();
        const closes = d2?.chart?.result?.[0]?.indicators?.quote?.[0]?.close || [];
        const price = closes.find((p: number) => p > 0) || d2?.chart?.result?.[0]?.meta?.regularMarketPrice || 0;

        return new Response(JSON.stringify({ ticker, date, price: parseFloat((price||0).toFixed(2)) }),
          { headers: { ...corsHeaders, "Content-Type": "application/json" } });
      } catch(e: any) {
        return new Response(JSON.stringify({ ticker, date, price: 0, error: e.message }),
          { headers: { ...corsHeaders, "Content-Type": "application/json" } });
      }
    }


    if (action === "live_indices") {
      // Fetch live values for all major indices from Yahoo Finance
      const INDICES: Record<string,string> = {
        US_SP500:    "^GSPC",   // S&P 500
        US_NASDAQ:   "^IXIC",   // Nasdaq Composite
        US_DOW:      "^DJI",    // Dow Jones
        SG_STI:      "^STI",    // Straits Times Index
        JP_NIKKEI:   "^N225",   // Nikkei 225
        CN_HSI:      "^HSI",    // Hang Seng
        EU_CAC:      "^FCHI",   // CAC 40
        EU_DAX:      "^GDAXI",  // DAX
        GB_FTSE:     "^FTSE",   // FTSE 100
        AU_ASX:      "^AXJO",   // ASX 200
      };

      const results: Record<string, any> = {};
      await Promise.allSettled(Object.entries(INDICES).map(async ([key, sym]) => {
        try {
          const url = `https://query1.finance.yahoo.com/v8/finance/chart/${encodeURIComponent(sym)}?interval=1d&range=1y`;
          const res = await yahooFetch(url, 8000);
          const meta = res?.chart?.result?.[0]?.meta;
          const closes: number[] = res?.chart?.result?.[0]?.indicators?.quote?.[0]?.close || [];
          const price = meta?.regularMarketPrice || 0;
          const prev  = meta?.chartPreviousClose   || 0;
          const change = prev > 0 ? ((price - prev) / prev * 100) : 0;
          // YTD: find first trading day of current year
          const firstOfYear = closes.find((c: number) => c > 0) || prev;
          const ytd = firstOfYear > 0 ? ((price - firstOfYear) / firstOfYear * 100) : 0;
          results[key] = {
            symbol: sym, price: +price.toFixed(2),
            change: +change.toFixed(2), ytd: +ytd.toFixed(2)
          };
        } catch(e: any) { results[key] = null; }
      }));

      console.log(`[live_indices] Fetched ${Object.values(results).filter(v=>v).length}/${Object.keys(INDICES).length}`);
      return new Response(JSON.stringify({ indices: results }),
        { headers: { ...corsHeaders, "Content-Type": "application/json" } });
    }

    if (action === "alerts") {
      // Three-signal alert engine:
      // 1. Insider buys via Finnhub (US stocks, last 30 days)
      // 2. Short squeeze proxy: shortRatio + shortPercent + price momentum (US)
      // 3. Volume spike: regularMarketVolume vs averageDailyVolume90Day (all markets)
      const { holdings: alertHoldings } = body;
      if (!Array.isArray(alertHoldings) || !alertHoldings.length) {
        return new Response(JSON.stringify({ alerts: [] }),
          { headers: { ...corsHeaders, "Content-Type": "application/json" } });
      }

      const alerts: any[] = [];
      const thirtyDaysAgo = new Date(Date.now() - 30 * 24 * 3600 * 1000).toISOString().slice(0,10);
      const ninetyDaysAgo = new Date(Date.now() - 90 * 24 * 3600 * 1000).toISOString().slice(0,10);

      // ── SIGNAL 1+2: Finnhub insider + short metrics (US stocks only) ──────
      const usHoldings = alertHoldings.filter((h: any) => h.mkt === "US");
      console.log(`[alerts] US holdings for Finnhub: ${usHoldings.length}`);

      for (let i = 0; i < usHoldings.length; i += 8) {
        const batch = usHoldings.slice(i, i + 8);
        await Promise.allSettled(batch.map(async (h: any) => {
          const t = h.ticker;
          try {
            // Insider transactions
            const insRes = await fetch(
              `https://finnhub.io/api/v1/stock/insider-transactions?symbol=${encodeURIComponent(t)}&token=${FH_KEY}`,
              { headers: { "User-Agent": "Mozilla/5.0" } }
            );
            if (insRes.ok) {
              const insData = await insRes.json();
              const recentBuys = (insData?.data || []).filter((tx: any) =>
                tx.transactionDate >= thirtyDaysAgo &&
                tx.transactionType === "P" && // P = Purchase
                tx.share > 0 &&
                tx.change > 0
              );
              if (recentBuys.length > 0) {
                const topBuy = recentBuys.sort((a: any, b: any) => b.share - a.share)[0];
                const totalShares = recentBuys.reduce((s: number, tx: any) => s + tx.share, 0);
                alerts.push({
                  type: "INSIDER_BUY",
                  ticker: t,
                  name: h.name || t,
                  mkt: h.mkt,
                  severity: totalShares > 50000 ? "high" : totalShares > 10000 ? "medium" : "low",
                  title: `Insider Buying: ${t}`,
                  detail: `${recentBuys.length} purchase${recentBuys.length>1?"s":""} in last 30 days · ${totalShares.toLocaleString()} shares total`,
                  who: topBuy.name || "Unknown",
                  date: topBuy.transactionDate,
                  value: Math.abs(topBuy.share * (topBuy.price || 0)),
                });
              }
            }
          } catch(e: any) { console.log(`[alerts] insider ${t}: ${e.message}`); }

          try {
            // Short metrics for squeeze detection
            const mRes = await fetch(
              `https://finnhub.io/api/v1/stock/metric?symbol=${encodeURIComponent(t)}&metric=all&token=${FH_KEY}`,
              { headers: { "User-Agent": "Mozilla/5.0" } }
            );
            if (mRes.ok) {
              const m = await mRes.json();
              const shortRatio  = m?.metric?.shortRatio || 0;          // days to cover
              const shortPct    = m?.metric?.shortInterestPercentOfFloat || 0; // % of float shorted
              const price52wLow = m?.metric?.["52WeekLow"] || 0;
              const price52wHigh= m?.metric?.["52WeekHigh"] || 0;
              const currentPrice= h.price || 0;
              // Price momentum: how far off 52w low vs high
              const pctFrom52wLow = price52wLow > 0 ? ((currentPrice - price52wLow) / price52wLow) * 100 : 0;

              // Short squeeze signal: heavy shorting + price recovering from lows
              if (shortPct > 15 && pctFrom52wLow > 20) {
                const severity = shortPct > 30 ? "high" : shortPct > 20 ? "medium" : "low";
                alerts.push({
                  type: "SHORT_SQUEEZE",
                  ticker: t,
                  name: h.name || t,
                  mkt: h.mkt,
                  severity,
                  title: `Short Squeeze Risk: ${t}`,
                  detail: `${shortPct.toFixed(1)}% of float shorted · ${shortRatio.toFixed(1)} days to cover · +${pctFrom52wLow.toFixed(0)}% off 52w low`,
                  shortPct,
                  shortRatio,
                  pctFrom52wLow,
                  date: new Date().toISOString().slice(0,10),
                });
              }
            }
          } catch(e: any) { console.log(`[alerts] short ${t}: ${e.message}`); }
        }));
        // Respect Finnhub 60 req/min — each holding = 2 calls, batch of 8 = 16 calls
        if (i + 8 < usHoldings.length) {
          await new Promise(resolve => setTimeout(resolve, 1200));
        }
      }

      // ── SIGNAL 3: Volume spike via Yahoo Finance (ALL markets) ─────────────
      // Build yahoo symbols for all holdings
      const allHoldings = alertHoldings;
      const tickerMktMap3: Record<string,string> = {};
      allHoldings.forEach((h: any) => { if (h.ticker && h.mkt) tickerMktMap3[h.ticker] = h.mkt; });

      // Chunk into 20 at a time for Yahoo bulk quote
      const chunkSize = 20;
      for (let i = 0; i < allHoldings.length; i += chunkSize) {
        const chunk = allHoldings.slice(i, i + chunkSize);
        const yahooToOrig: Record<string,string> = {};
        const yahooSyms = chunk.map((h: any) => {
          const yt = yahooTicker(h.ticker, h.mkt);
          yahooToOrig[yt] = h.ticker;
          yahooToOrig[h.ticker] = h.ticker;
          return yt;
        });

        try {
          const url = `https://query2.finance.yahoo.com/v7/finance/quote?symbols=${encodeURIComponent(yahooSyms.join(","))}`;
          const res = await fetch(url, {
            headers: {
              "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
              "Accept": "application/json",
              "Referer": "https://finance.yahoo.com/",
            }
          });
          if (!res.ok) continue;
          const d = await res.json();
          const quotes = d?.quoteResponse?.result || [];
          quotes.forEach((q: any) => {
            const sym = q?.symbol || "";
            const origTicker = yahooToOrig[sym] || yahooToOrig[sym.toUpperCase()] || sym;
            const h = allHoldings.find((x: any) => x.ticker === origTicker);
            if (!h) return;

            const vol    = q?.regularMarketVolume || 0;
            const avg90d = q?.averageDailyVolume3Month || q?.averageDailyVolume10Day || 0;
            const price  = q?.regularMarketPrice || 0;
            const chg1d  = q?.regularMarketChangePercent || 0;

            if (avg90d > 0 && vol > 0) {
              const volMultiple = vol / avg90d;
              // Volume spike: 3x average AND (price move OR large absolute spike)
              if (volMultiple >= 3.0) {
                const severity = volMultiple >= 6 ? "high" : volMultiple >= 4 ? "medium" : "low";
                alerts.push({
                  type: "VOLUME_SPIKE",
                  ticker: origTicker,
                  name: h.name || origTicker,
                  mkt: h.mkt,
                  severity,
                  title: `Volume Spike: ${origTicker}`,
                  detail: `${volMultiple.toFixed(1)}× normal volume · ${chg1d >= 0 ? "+" : ""}${chg1d.toFixed(1)}% price move · ${(vol/1e6).toFixed(1)}M vs ${(avg90d/1e6).toFixed(1)}M avg`,
                  volMultiple,
                  chg1d,
                  date: new Date().toISOString().slice(0,10),
                });
              }
            }
          });
        } catch(e: any) { console.log(`[alerts] volume chunk ${i}: ${e.message}`); }
      }

      // Sort by severity (high first) then date (most recent first)
      const sevOrder: Record<string,number> = { high: 0, medium: 1, low: 2 };
      alerts.sort((a, b) => {
        const sevDiff = (sevOrder[a.severity] || 2) - (sevOrder[b.severity] || 2);
        if (sevDiff !== 0) return sevDiff;
        return (b.date || "").localeCompare(a.date || "");
      });

      // Cap each signal type to top 10 so no single type dominates
      const byType: Record<string, any[]> = {};
      alerts.forEach(a => {
        if (!byType[a.type]) byType[a.type] = [];
        if (byType[a.type].length < 10) byType[a.type].push(a);
      });
      // Re-interleave: group by severity across types
      const cappedAlerts = Object.values(byType).flat()
        .sort((a, b) => (sevOrder[a.severity] || 2) - (sevOrder[b.severity] || 2));

      console.log(`[alerts] Generated ${cappedAlerts.length} alerts (${usHoldings.length} US + ${allHoldings.length - usHoldings.length} non-US checked)`);
      return new Response(JSON.stringify({ alerts: cappedAlerts }),
        { headers: { ...corsHeaders, "Content-Type": "application/json" } });
    }

    if (action === "screen") {
      // God Mode Screener — fetches fresh data for scoring:
      // 1. RSI-14 from Yahoo Finance price history (all markets)
      // 2. Fundamentals: revenueGrowth, D/E ratio (Finnhub, US/EU only)
      // 3. Analyst consensus rating (Finnhub, US only)
      // 4. 52-week range position (Finnhub metric)
      const { holdings: screenHoldings } = body;
      if (!Array.isArray(screenHoldings) || !screenHoldings.length) {
        return new Response(JSON.stringify({ screenData: {} }),
          { headers: { ...corsHeaders, "Content-Type": "application/json" } });
      }

      const tickerMktMap4: Record<string,string> = {};
      screenHoldings.forEach((h: any) => { if (h.ticker && h.mkt) tickerMktMap4[h.ticker] = h.mkt; });

      const screenData: Record<string, any> = {};

      // ── RSI-14 + momentum via Yahoo chart (all markets) ──────────────────
      const concRSI = 10;
      for (let i = 0; i < screenHoldings.length; i += concRSI) {
        const batch = screenHoldings.slice(i, i + concRSI);
        await Promise.allSettled(batch.map(async (h: any) => {
          const t = h.ticker;
          const mkt = h.mkt || "US";
          try {
            const yt = yahooTicker(t, mkt);
            // 3-month daily data for RSI-14 and momentum
            const url = `https://query2.finance.yahoo.com/v8/finance/chart/${encodeURIComponent(yt)}?interval=1d&range=3mo`;
            const ctrl = new AbortController();
            const tm = setTimeout(() => ctrl.abort(), 10000);
            const res = await fetch(url, {
              signal: ctrl.signal,
              headers: { "User-Agent": "Mozilla/5.0", "Accept": "application/json", "Referer": "https://finance.yahoo.com/" }
            });
            clearTimeout(tm);
            if (!res.ok) return;
            const d = await res.json();
            const closes = d?.chart?.result?.[0]?.indicators?.quote?.[0]?.close?.filter((x: any) => x != null) || [];
            const meta = d?.chart?.result?.[0]?.meta || {};

            // Compute RSI-14
            let rsi = 50; // neutral default
            if (closes.length >= 15) {
              let gains = 0, losses = 0;
              for (let k = closes.length - 14; k < closes.length; k++) {
                const chg = closes[k] - closes[k-1];
                if (chg > 0) gains += chg; else losses -= chg;
              }
              const avgGain = gains / 14;
              const avgLoss = losses / 14;
              rsi = avgLoss === 0 ? 100 : parseFloat((100 - 100 / (1 + avgGain / avgLoss)).toFixed(1));
            }

            // Price momentum: 1-month and 3-month return
            const n = closes.length;
            const mom1m = n >= 21 ? ((closes[n-1] - closes[n-21]) / closes[n-21]) * 100 : null;
            const mom3m = n >= 3  ? ((closes[n-1] - closes[0])    / closes[0])    * 100 : null;

            // 52-week position from meta
            const hi52 = (meta as any)["52WeekHigh"] || 0;
            const lo52 = (meta as any)["52WeekLow"]  || 0;
            const price = (meta as any).regularMarketPrice || closes[n-1] || 0;
            const pctFromHi = hi52 > 0 ? ((price - hi52) / hi52) * 100 : null;
            const pctFromLo = lo52 > 0 ? ((price - lo52) / lo52) * 100 : null;

            if (!screenData[t]) screenData[t] = {};
            Object.assign(screenData[t], { rsi, mom1m, mom3m, hi52, lo52, price52: price, pctFromHi, pctFromLo });
          } catch { /* silent */ }
        }));
      }

      // ── Fundamentals via Finnhub (US + EU stocks) ─────────────────────────
      const fundamentalHoldings = screenHoldings.filter((h: any) => ["US","EU","GB"].includes(h.mkt || ""));
      console.log(`[screen] Fundamentals for ${fundamentalHoldings.length} US/EU tickers via Finnhub`);

      for (let i = 0; i < fundamentalHoldings.length; i += 8) {
        const batch = fundamentalHoldings.slice(i, i + 8);
        await Promise.allSettled(batch.map(async (h: any) => {
          const t = h.ticker;
          try {
            const r = await fetch(
              `https://finnhub.io/api/v1/stock/metric?symbol=${encodeURIComponent(t)}&metric=all&token=${FH_KEY}`,
              { headers: { "User-Agent": "Mozilla/5.0" } }
            );
            if (!r.ok) return;
            const m = await r.json();
            const metric = m?.metric || {};

            const revenueGrowth  = metric.revenueGrowthTTMYoy || metric.revenueGrowth3Y || null;
            const debtToEquity   = metric.totalDebt2EquityAnnual || metric["longTermDebt/equityAnnual"] || null;
            const epsGrowth      = metric.epsGrowth3Y || metric.epsGrowthTTMYoy || null;
            const currentRatio   = metric.currentRatioAnnual || null;
            const grossMargin    = metric.grossMarginTTM || null;
            const roe            = metric.roeRfy || metric.roe5Y || null;

            if (!screenData[t]) screenData[t] = {};
            Object.assign(screenData[t], { revenueGrowth, debtToEquity, epsGrowth, currentRatio, grossMargin, roe });
          } catch { /* silent */ }
        }));
        if (i + 8 < fundamentalHoldings.length) {
          await new Promise(resolve => setTimeout(resolve, 1100));
        }
      }

      // ── Analyst consensus (Finnhub, US only) ─────────────────────────────
      const usHoldingsS = screenHoldings.filter((h: any) => h.mkt === "US");
      for (let i = 0; i < usHoldingsS.length; i += 8) {
        const batch = usHoldingsS.slice(i, i + 8);
        await Promise.allSettled(batch.map(async (h: any) => {
          const t = h.ticker;
          try {
            const r = await fetch(
              `https://finnhub.io/api/v1/stock/recommendation?symbol=${encodeURIComponent(t)}&token=${FH_KEY}`,
              { headers: { "User-Agent": "Mozilla/5.0" } }
            );
            if (!r.ok) return;
            const data = await r.json();
            if (data && data.length > 0) {
              const latest = data[0];
              const total  = (latest.strongBuy||0) + (latest.buy||0) + (latest.hold||0) + (latest.sell||0) + (latest.strongSell||0);
              const buyPct = total > 0 ? ((latest.strongBuy||0) + (latest.buy||0)) / total * 100 : null;
              if (!screenData[t]) screenData[t] = {};
              Object.assign(screenData[t], {
                analystBuyPct: buyPct,
                analystBuy: latest.buy||0,
                analystHold: latest.hold||0,
                analystSell: (latest.sell||0) + (latest.strongSell||0),
                analystTotal: total,
              });
            }
          } catch { /* silent */ }
        }));
        if (i + 8 < usHoldingsS.length) {
          await new Promise(resolve => setTimeout(resolve, 1100));
        }
      }

      const covered = Object.keys(screenData).length;
      console.log(`[screen] Complete: ${covered}/${screenHoldings.length} stocks have screen data`);
      return new Response(JSON.stringify({ screenData }),
        { headers: { ...corsHeaders, "Content-Type": "application/json" } });
    }

    if (action === "ticker_search") {
      // 3-layer ticker search for broker message parser
      // Layer 1: Yahoo Finance autocomplete API (fastest, all markets)
      // Layer 2: Returns multiple matches ranked by relevance
      const { query, mkt } = body;
      if (!query || query.trim().length < 2) {
        return new Response(JSON.stringify({ results: [] }),
          { headers: { ...corsHeaders, "Content-Type": "application/json" } });
      }

      const results: any[] = [];

      // ── Layer 1: Yahoo Finance autocomplete ──────────────────────────────────
      // Supports ALL markets: JP (.T), SG (.SI), HK (.HK), US, EU etc.
      try {
        const q = encodeURIComponent(query.trim());
        const url = `https://query2.finance.yahoo.com/v1/finance/search?q=${q}&quotesCount=8&newsCount=0&enableFuzzyQuery=true&enableCb=true&enableNavLinks=false`;
        const ctrl = new AbortController();
        const tm = setTimeout(() => ctrl.abort(), 8000);
        const res = await fetch(url, {
          signal: ctrl.signal,
          headers: {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
            "Accept": "application/json",
            "Referer": "https://finance.yahoo.com/",
          }
        });
        clearTimeout(tm);
        if (res.ok) {
          const d = await res.json();
          const quotes = d?.quotes || [];
          quotes.forEach((q: any) => {
            if (!q.symbol || !q.shortname) return;
            // Filter to relevant markets if mkt specified
            const sym = q.symbol;
            const exchDisp = q.exchDisp || q.exchange || "";
            const typeDisp = q.typeDisp || "";
            // Only include equities and ETFs (skip news, crypto, futures)
            if (!["Equity","ETF","Fund"].includes(typeDisp) && typeDisp !== "") return;
            // Map Yahoo exchange to Ignitus mkt code
            let igMkt = "US";
            if (sym.endsWith(".T")) igMkt = "JP";
            else if (sym.endsWith(".SI")) igMkt = "SG";
            else if (sym.endsWith(".HK")) igMkt = "CN";
            else if (sym.endsWith(".AX")) igMkt = "AU";
            else if (sym.endsWith(".L")) igMkt = "EU";
            else if (sym.endsWith(".PA") || sym.endsWith(".DE") || sym.endsWith(".AS")) igMkt = "EU";
            else if (!sym.includes(".")) igMkt = "US";

            // Score: prefer if market matches requested mkt
            const mktMatch = !mkt || igMkt === mkt;
            results.push({
              ticker: sym,
              name: q.longname || q.shortname || "",
              exchange: exchDisp,
              mkt: igMkt,
              score: (mktMatch ? 10 : 0) + (q.score || 0),
              source: "yahoo",
            });
          });
        }
        console.log(`[ticker_search] Yahoo: ${results.length} results for "${query}"`);
      } catch(e: any) {
        console.log(`[ticker_search] Yahoo error: ${e.message}`);
      }

      // Sort by score descending, deduplicate by ticker
      const seen = new Set<string>();
      const deduped = results
        .sort((a, b) => b.score - a.score)
        .filter(r => { if (seen.has(r.ticker)) return false; seen.add(r.ticker); return true; })
        .slice(0, 6);

      console.log(`[ticker_search] Final: ${deduped.length} results`);
      return new Response(JSON.stringify({ results: deduped }),
        { headers: { ...corsHeaders, "Content-Type": "application/json" } });
    }

    if (action === "valuation") {
      // Multi-source valuation: Analyst Target + DCF (FCF) + DCF (EPS) + Peter Lynch
      const FH_KEY = "d7hji19r01qhiu0brkigd7hji19r01qhiu0brkj0";
      const ticker: string = body.ticker || "";
      if (!ticker) {
        return new Response(JSON.stringify({ error: "ticker required" }),
          { headers: { ...corsHeaders, "Content-Type": "application/json" } });
      }

      try {
        // Fetch in parallel: price target, metrics, quote, recommendation + FMP DCF
        const FMP_KEY = "ZH24wvUKb1HcQIlEkGE8DshWjiefo38r";
        const [tRes, mRes, qRes, rRes, fmpRes] = await Promise.all([
          fetch(`https://finnhub.io/api/v1/stock/price-target?symbol=${ticker}&token=${FH_KEY}`),
          fetch(`https://finnhub.io/api/v1/stock/metric?symbol=${ticker}&metric=all&token=${FH_KEY}`),
          fetch(`https://finnhub.io/api/v1/quote?symbol=${ticker}&token=${FH_KEY}`),
          fetch(`https://finnhub.io/api/v1/stock/recommendation?symbol=${ticker}&token=${FH_KEY}`),
          fetch(`https://financialmodelingprep.com/api/v3/discounted-cash-flow/${encodeURIComponent(ticker)}?apikey=${FMP_KEY}`),
        ]);

        const safeJson = async (res: Response, fallback: any) => {
          if (!res.ok) return fallback;
          try { return await res.json(); } catch { return fallback; }
        };
        const t = await safeJson(tRes, {});
        const m = await safeJson(mRes, {});
        const q = await safeJson(qRes, {});
        const r = await safeJson(rRes, []);
        // FMP DCF: returns array [{symbol, date, dcf, "Stock Price"}]
        const fmpData = await safeJson(fmpRes, []);
        const fmpDcf  = Array.isArray(fmpData) && fmpData[0]?.dcf > 0
          ? +parseFloat(fmpData[0].dcf).toFixed(2)
          : 0;
        console.log(`[valuation] FMP DCF for ${ticker}: ${fmpDcf}`);

        const currentPrice = q.c || 0;
        const metric       = m.metric || {};

        // ── 1. Analyst consensus (Wall Street median) ──────────────────────────
        const analystTarget = t.targetMean || t.targetMedian || 0;
        const analystHigh   = t.targetHigh  || 0;
        const analystLow    = t.targetLow   || 0;
        const numAnalysts   = t.numberOfAnalysts || 0;

        // ── 2. Core inputs ────────────────────────────────────────────────────
        const eps        = metric.epsBasicExclExtraItemsTTM || metric.epsTTM || 0;
        const fcfPerShare= metric.freeCashFlowPerShareTTM   || metric.fcfShareTTM || metric.fcfPerShareTTM || 0;

        // Growth rate: real 5yr EPS growth from Finnhub, capped 2%–15% for sanity
        const rawGrowthPct = metric.epsGrowth5Y || metric.epsGrowth3Y || metric.revenueGrowth5Y || 5.0;
        // FIX 3: Raised growth cap 15%→25% — NVDA/CRWD/MELI had >20% actual 5yr EPS CAGR
        const growthPct    = Math.max(2.0, Math.min(25.0, rawGrowthPct));
        const g            = growthPct / 100;   // decimal form
        // FIX 2: Market-aware discount rates — flatter 10% ignores market risk premia
        // US large-cap equity: 10%, SG/HK/EU mature markets: 8-9%, JP low-rate: 7%
        // Fallback 10% for unknowns
        const mktR2: Record<string,number> = {US:0.10, SG:0.08, CN:0.09, HK:0.09, JP:0.07, EU:0.09, GB:0.09};
        const r2           = mktR2[body?.mkt || "US"] || 0.10; // required return
        const t2           = 0.025;             // terminal growth rate

        // Helper: 5-year explicit DCF + Gordon Growth terminal value
        const calcDCF = (base: number): number => {
          if (base <= 0) return 0;  // g can exceed r2 since terminal uses t2=2.5%, not g
          let sum = 0;
          for (let yr = 1; yr <= 5; yr++) {
            sum += base * Math.pow(1 + g, yr) / Math.pow(1 + r2, yr);
          }
          const tv = base * Math.pow(1 + g, 5) * (1 + t2) / (r2 - t2);
          sum += tv / Math.pow(1 + r2, 5);
          // Sanity cap: no more than 5× current price to prevent explosive results
          if (currentPrice > 0 && sum > currentPrice * 5) sum = currentPrice * 5;
          return sum;
        };

        // ── 3. DCF (FCF-based) ────────────────────────────────────────────────
        // Projects free cash flow per share forward, discounts back
        const dcfFCF = calcDCF(fcfPerShare);

        // ── 4. DCF (Earnings-based) ───────────────────────────────────────────
        // Same model but uses EPS — GuruFocus approach, historically stronger correlation
        const dcfEPS = calcDCF(eps);

        // ── 5. Peter Lynch Fair Value ─────────────────────────────────────────
        // Lynch: fairly valued stock has P/E = growth rate (PEG = 1.0)
        // Fair Price = EPS × growth_rate_percent
        // Example: EPS=$10, growth=15% → Fair P/E=15 → Fair Price=$150
        const peterLynch = eps > 0 && growthPct > 0 ? +(eps * growthPct).toFixed(2) : 0;

        // ── 6. Recommendation consensus ───────────────────────────────────────
        const latestRec    = Array.isArray(r) && r[0] ? r[0] : {};
        const sb           = latestRec.strongBuy  || 0;
        const bu           = latestRec.buy        || 0;
        const hd           = latestRec.hold       || 0;
        const sl           = latestRec.sell       || 0;
        const ss           = latestRec.strongSell || 0;
        const recTotal     = sb + bu + hd + sl + ss;
        const recScore     = recTotal > 0 ? +((sb*2 + bu - sl - ss*2) / recTotal).toFixed(2) : 0;

        // ── 7. Average of available (non-zero) estimates — excludes Analyst Target ──
        const estimates = [fmpDcf, dcfFCF, dcfEPS, peterLynch].filter(v => v > 0);
        const avg       = estimates.length > 0
          ? +(estimates.reduce((s, v) => s + v, 0) / estimates.length).toFixed(2)
          : 0;

        const result = {
          ticker,
          currentPrice: +currentPrice.toFixed(2),
          valuations: {
            analystTarget: +analystTarget.toFixed(2),
            analystHigh:   +analystHigh.toFixed(2),
            analystLow:    +analystLow.toFixed(2),
            numAnalysts,
            dcfFCF:        +dcfFCF.toFixed(2),
            dcfEPS:        +dcfEPS.toFixed(2),
            peterLynch:    +peterLynch.toFixed(2),
            fmpDcf:        fmpDcf,
            average:       avg,
          },
          inputs: {
            eps,
            fcfPerShare,
            pe:         metric.peBasicExclExtraTTM || metric.peTTM || 0,
            divYield:   metric.dividendYieldIndicatedAnnual || 0,
            growthUsed: +growthPct.toFixed(1),
            growthSource: metric.epsGrowth5Y ? "5yr EPS growth"
                        : metric.epsGrowth3Y ? "3yr EPS growth (5yr unavailable)"
                        : metric.revenueGrowth5Y ? "5yr revenue growth (EPS unavailable)"
                        : "default 5% (no growth data)",
          },
          recommendation: {
            strongBuy: sb, buy: bu, hold: hd, sell: sl, strongSell: ss,
            period:        latestRec.period || "",
            totalAnalysts: recTotal,
            score:         recScore,
          },
          dataAvailability: {
            analystTargetAvailable: analystTarget > 0,
            dcfFCFAvailable:        dcfFCF > 0,
            dcfEPSAvailable:        dcfEPS > 0,
            peterLynchAvailable:    peterLynch > 0,
            fmpDcfAvailable:        fmpDcf > 0,
            recommendationAvailable:recTotal > 0,
          },
          assumptions: {
            dcfFCF:      `FCF/share ($${fcfPerShare.toFixed(2)}) × ${growthPct.toFixed(1)}% growth / 10% disc. / 2.5% terminal`,
            dcfEPS:      `EPS ($${eps.toFixed(2)}) × ${growthPct.toFixed(1)}% growth / 10% disc. / 2.5% terminal`,
            peterLynch:  `EPS ($${eps.toFixed(2)}) × ${growthPct.toFixed(1)} = PEG 1.0 (Peter Lynch)`,
            analyst:     `Median of ${numAnalysts} Wall Street analyst 12-month price targets`,
          },
        };

        console.log(`[valuation] ${ticker}: analyst=${analystTarget} dcfFCF=${dcfFCF.toFixed(0)} dcfEPS=${dcfEPS.toFixed(0)} lynch=${peterLynch.toFixed(0)} fmpDcf=${fmpDcf} avg=${avg} growth=${growthPct.toFixed(1)}%`);
        return new Response(JSON.stringify(result),
          { headers: { ...corsHeaders, "Content-Type": "application/json" } });

      } catch(e: any) {
        console.error(`[valuation] Failed: ${e.message}`);
        return new Response(JSON.stringify({ error: e.message }),
          { headers: { ...corsHeaders, "Content-Type": "application/json" } });
      }
    }


    return new Response(JSON.stringify({ error: "unknown action" }),
      { status: 400, headers: { ...corsHeaders, "Content-Type": "application/json" } });

  } catch(e: any) {
    console.error("Error:", e.message);
    return new Response(JSON.stringify({ error: e.message }),
      { status: 500, headers: { ...corsHeaders, "Content-Type": "application/json" } });
  }
});
