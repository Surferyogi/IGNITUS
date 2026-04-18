// ── Electron DB bridge ────────────────────────────────────────────────────────
// Data is loaded from SQLite via window.portfolioDB (exposed by preload.js)
// ALL_H / ALL_T / SENATE / FIRST_BUY are populated from the database at startup

const { useState, useMemo, useEffect, useCallback } = React;

// Static fallbacks — replaced immediately by live DB data in App useEffect
let ALL_H = [];
let ALL_T = [];
let SENATE = [];
let FIRST_BUY = {};


const C={bg:"#0A0D14",surface:"#111620",card:"#161C2A",border:"#1E2A3E",accent:"#00D4FF",accentDim:"#0099BB",green:"#00E5A0",red:"#FF4D6A",gold:"#FFB547",purple:"#9B6DFF",text:"#E8EDF5",muted:"#6B7A99",mutedLight:"#8B97B3"};
const MKT={
  US:{symbol:"$",  code:"USD",r:1.36, index:"S&P 500",   idxVal:5463.2,  idxYtd:14.8, idxChange:1.24},
  JP:{symbol:"¥",  code:"JPY",r:0.0087,index:"Nikkei 225",idxVal:35808,   idxYtd:-4.2, idxChange:0.78},
  EU:{symbol:"€",  code:"EUR",r:1.60, index:"CAC 40",    idxVal:7842.3,  idxYtd:4.1,  idxChange:0.62},
  CN:{symbol:"HK$",code:"HKD",r:0.17, index:"HSI",       idxVal:18042.7, idxYtd:8.3,  idxChange:0.54},
  GB:{symbol:"£",  code:"GBP",r:1.69, index:"FTSE 100",  idxVal:8218.3,  idxYtd:5.1,  idxChange:-0.31},
  AU:{symbol:"A$", code:"AUD",r:0.87, index:"ASX 200",   idxVal:7834.1,  idxYtd:3.7,  idxChange:-0.12},
  SG:{symbol:"S$", code:"SGD",r:1.0,  index:"STI",       idxVal:3892.4,  idxYtd:6.2,  idxChange:0.41},
};
const fmt=(n,d=2)=>n==null?"--":n.toLocaleString("en-US",{minimumFractionDigits:d,maximumFractionDigits:d});
const fmtPct=n=>n==null?"--":(n>=0?"+":"")+fmt(n)+"%";
const toSGD=(v,mkt)=>v*(MKT[mkt]?.r??1.36);
const fmtL=(n,mkt,d=2)=>n==null?"--":(MKT[mkt]?.symbol??"$")+fmt(Math.abs(n),d);
const fmtS=(n,d=0)=>n==null?"--":"S$"+fmt(Math.abs(n),d);
const SECTORS=["Technology","Healthcare","Financials","Consumer Disc.","Industrials","Energy","Utilities","Materials","Real Estate","Comm. Services","Consumer Staples"];
const MS_STYLES=["Large Growth","Large Blend","Large Value","Mid Growth","Mid Blend","Mid Value","Small Growth","Small Blend","Small Value"];
const SCOL=[C.accent,C.green,C.gold,C.purple,"#FF8C42","#FF4D6A","#62D2E8","#C084FC","#FDE68A","#A3E635","#FB923C"];


// Seeded RNG - no < operators
const sr=seed=>{let s=seed%2147483647;return()=>{s=(s*16807)%2147483647;return(s-1)/2147483646;};};

// Price history generator - uses Array.from to avoid for-loop with i<days
const gH=(base,seed,days)=>{
  const r=sr(seed);
  let v=base*(1-(days/3650)*0.8);
  const pts=Array.from({length:days},()=>{v=v*(1+(r()-0.47)*0.018);return +v.toFixed(4);});
  pts.push(base);
  return pts;
};

// Pre-build histories for each period per holding
const HIST={};
ALL_H.forEach((h,i)=>{
  HIST[h.ticker]={};
  const daysMap={"30d":30,"6m":180,"1y":365,"5y":1825};
  ["30d","6m","1y","5y"].forEach(p=>{
    HIST[h.ticker][p]=gH(h.price, i*13+p.charCodeAt(0), daysMap[p]);
  });
  const firstDate=new Date(FIRST_BUY[h.ticker]||"2020-01-01");
  const sinceMs=new Date("2026-04-17")-firstDate;
  const sinceDays=Math.max(30,Math.round(sinceMs/86400000));
  HIST[h.ticker]["all"]=gH(h.price, i*13+97, sinceDays);
});

// Scoring - avoid bare < in expressions by using negation where possible
const scoreH=h=>{
  const up=((h.intrinsic-h.price)/h.price)*100;
  const iv=Math.max(0,Math.min(10,Math.round(5+up/10)));
  const mt=h.moat==="Wide"?9:h.moat==="Narrow"?6:3;
  const dv=Math.min(10,Math.round(h.divYield*1.5+2));
  return{iv,mt,dv,all:Math.round(iv*0.4+mt*0.35+dv*0.25)};
};
const getRec=h=>{
  const up=((h.intrinsic-h.price)/h.price)*100;
  if(up>15&&h.moat!=="None")return{lbl:"STRONG BUY",col:C.green};
  if(up>5)return{lbl:"BUY",col:"#72E5A0"};
  if(up>-10)return{lbl:"HOLD",col:C.gold};
  return{lbl:"SELL",col:C.red};
};
const buffettScore=h=>{
  const gainPct=((h.price-h.avgCost)/h.avgCost)*100;
  const upside=((h.intrinsic-h.price)/h.price)*100;
  const moatPts=h.moat==="Wide"?30:h.moat==="Narrow"?15:0;
  const divPts=Math.min(20,h.divYield*4);
  const valuePts=upside>20?25:upside>10?15:upside>0?8:0;
  const pe=h.peRatio;
  const qualPts=(pe>0&&25>pe)?15:(35>pe)?8:0;
  const gainPts=gainPct>50?10:gainPct>20?5:0;
  const total=moatPts+divPts+valuePts+qualPts+gainPts;
  let action,reason,col;
  if(total>=65&&upside>10){action="BUY MORE";col=C.green;reason="Wide moat + undervalued";}
  else if(total>=50&&upside>0){action="ADD GRADUALLY";col="#72E5A0";reason="Good fundamentals, fair value";}
  else if(total>=35&&upside>-10){action="HOLD";col=C.gold;reason="Solid business, fairly priced";}
  else if(upside>-15===false||h.moat==="None"){action="CONSIDER SELLING";col=C.red;reason="Overvalued or weak moat";}
  else{action="WATCH";col=C.mutedLight;reason="Monitor for better entry";}
  return{score:total,action,reason,col};
};

// ─── Shared UI atoms ──────────────────────────────────────────────────────────
function Bdg({label,bg,color}){
  return <span style={{fontSize:9,fontWeight:700,padding:"2px 6px",borderRadius:3,background:bg,color}}>{label}</span>;
}
function Chip({mkt}){
  return <span style={{fontSize:9,fontWeight:700,padding:"1px 5px",borderRadius:3,background:C.accent+"18",color:C.accent}}>{MKT[mkt]?.code||mkt}</span>;
}
function Tag({col,children}){
  return <span style={{display:"inline-block",padding:"2px 6px",borderRadius:3,fontSize:9,fontWeight:700,background:col+"20",color:col}}>{children}</span>;
}
function ScoreBar({score,max=10,color}){
  const pct=(score/max)*100;
  const col=color||(pct>=70?C.green:pct>=40?C.gold:C.red);
  return(
    <div style={{display:"flex",alignItems:"center",gap:8}}>
      <div style={{flex:1,height:4,borderRadius:2,background:C.border,overflow:"hidden"}}>
        <div style={{width:`${pct}%`,height:"100%",background:col,borderRadius:2}}/>
      </div>
      <span style={{fontSize:11,fontWeight:700,color:col,minWidth:14,textAlign:"right"}}>{score}</span>
    </div>
  );
}

// Sparkline — no bare < in this component
function Sparkline({data,color=C.accent,height=44}){
  if(!data||data.length<2)return null;
  const mn=Math.min(...data),mx=Math.max(...data),range=mx-mn||1;
  const W=300,H=height;
  const pts=data.map((v,i)=>[
    (i/(data.length-1))*W,
    H-((v-mn)/range)*H*0.82-H*0.09
  ]);
  const path=pts.map((p,i)=>`${i===0?"M":"L"}${p[0].toFixed(1)},${p[1].toFixed(1)}`).join(" ");
  const area=path+` L${W},${H} L0,${H} Z`;
  const gid="g"+color.replace(/[^a-zA-Z0-9]/g,"");
  return(
    <svg width="100%" viewBox={`0 0 ${W} ${H}`} style={{display:"block"}}>
      <defs>
        <linearGradient id={gid} x1="0" y1="0" x2="0" y2="1">
          <stop offset="0%" stopColor={color} stopOpacity="0.25"/>
          <stop offset="100%" stopColor={color} stopOpacity="0"/>
        </linearGradient>
      </defs>
      <path d={area} fill={`url(#${gid})`}/>
      <path d={path} fill="none" stroke={color} strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round"/>
    </svg>
  );
}

function MiniSparkline({data,color=C.accent}){
  if(!data||data.length<2)return null;
  const mn=Math.min(...data),mx=Math.max(...data),range=mx-mn||1;
  const W=80,H=30;
  const pts=data.map((v,i)=>[(i/(data.length-1))*W,H-((v-mn)/range)*H*0.82-H*0.09]);
  const path=pts.map((p,i)=>`${i===0?"M":"L"}${p[0].toFixed(1)},${p[1].toFixed(1)}`).join(" ");
  return(
    <svg width={W} height={H} viewBox={`0 0 ${W} ${H}`} style={{display:"block"}}>
      <path d={path} fill="none" stroke={color} strokeWidth="1.5" strokeLinecap="round"/>
    </svg>
  );
}

// Performance chart with period support
function PerfChart({mktFilter,period}){
  const subset=mktFilter==="ALL"?ALL_H:ALL_H.filter(h=>h.mkt===mktFilter);
  const m=MKT[mktFilter];
  const idxName=mktFilter==="ALL"?"S&P 500":(m?.index||"Index");
  const idxYtd=mktFilter==="ALL"?14.8:(m?.idxYtd||0);
  const rng=sr(mktFilter.charCodeAt(0)*7+period.charCodeAt(0));
  const refData=subset.length>0?HIST[subset[0].ticker]?.[period]:null;
  const days=(refData?.length||61)-1;
  const iH=[];
  let idxV=10000*(1-idxYtd/100*0.6);
  Array.from({length:days+1},(_,i)=>{idxV=idxV*(1+(rng()-0.47)*0.012);iH.push(idxV);});
  const iScale=(1+idxYtd/100)*10000/(iH[iH.length-1]||1);
  const iN=iH.map(v=>v*iScale);
  const pH=Array.from({length:days+1},(_,i)=>
    subset.reduce((s,h)=>{
      const hist=HIST[h.ticker]?.[period]||[];
      const idx=Math.round((i/days)*(hist.length-1));
      return s+toSGD((hist[idx]||h.price)*h.shares,h.mkt);
    },0)
  );
  const p0=pH[0]||1,i0=iN[0]||1;
  const pN=pH.map(v=>(v/p0)*100),iNorm=iN.map(v=>(v/i0)*100);
  const W=300,H=130;
  const allV=[...pN,...iNorm],mn=Math.min(...allV)-3,mx=Math.max(...allV)+3;
  const toY=v=>H-((v-mn)/(mx-mn))*H*0.88-H*0.06;
  const toX=i=>(i/days)*W;
  const pp=pN.map((v,i)=>`${i===0?"M":"L"}${toX(i).toFixed(1)},${toY(v).toFixed(1)}`).join(" ");
  const ip=iNorm.map((v,i)=>`${i===0?"M":"L"}${toX(i).toFixed(1)},${toY(v).toFixed(1)}`).join(" ");
  const pL=pN[pN.length-1],iL=iNorm[iNorm.length-1];
  const pR=(pL-100).toFixed(1),iR=(iL-100).toFixed(1);
  const PLBL={"30d":"30 Days","6m":"6 Months","1y":"1 Year","5y":"5 Years","all":"Since First Buy"};
  return(
    <div>
      <div style={{fontSize:10,color:C.muted,marginBottom:5}}>{PLBL[period]||period}</div>
      <div style={{display:"flex",gap:16,marginBottom:8,fontSize:11}}>
        <span style={{color:C.accent}}>Portfolio <b style={{color:+pR>=0?C.green:C.red}}>{+pR>=0?"+":""}{pR}%</b></span>
        <span style={{color:C.mutedLight}}>{idxName} <b style={{color:+iR>=0?C.green:C.red}}>{+iR>=0?"+":""}{iR}%</b></span>
      </div>
      <svg width="100%" viewBox={`0 0 ${W} ${H}`} style={{display:"block"}}>
        <defs>
          <linearGradient id="pGrMain" x1="0" y1="0" x2="0" y2="1"><stop offset="0%" stopColor={C.accent} stopOpacity="0.22"/><stop offset="100%" stopColor={C.accent} stopOpacity="0"/></linearGradient>
          <linearGradient id="iGrMain" x1="0" y1="0" x2="0" y2="1"><stop offset="0%" stopColor={C.mutedLight} stopOpacity="0.1"/><stop offset="100%" stopColor={C.mutedLight} stopOpacity="0"/></linearGradient>
        </defs>
        {[25,50,75].map(p=>{const y=toY(mn+(mx-mn)*(p/100));return <line key={p} x1={0} y1={y} x2={W} y2={y} stroke={C.border} strokeWidth="0.5" strokeDasharray="4,4"/>;})}
        <path d={ip+` L${W},${H} L0,${H} Z`} fill="url(#iGrMain)"/>
        <path d={ip} fill="none" stroke={C.mutedLight} strokeWidth="1" strokeDasharray="5,3" opacity="0.6"/>
        <path d={pp+` L${W},${H} L0,${H} Z`} fill="url(#pGrMain)"/>
        <path d={pp} fill="none" stroke={C.accent} strokeWidth="2" strokeLinecap="round"/>
        <circle cx={toX(days)} cy={toY(pL)} r="3.5" fill={C.accent}/>
        <circle cx={toX(days)} cy={toY(iL)} r="3" fill={C.mutedLight} opacity="0.7"/>
      </svg>
      <div style={{display:"flex",justifyContent:"space-between",fontSize:9,color:C.muted,marginTop:3}}>
        <span>{PLBL[period]||period} ago</span><span>Today</span>
      </div>
    </div>
  );
}

function DonutChart({data,size=96}){
  const total=data.reduce((s,d)=>s+d.value,0)||1;
  let cum=-90;
  const r=38,cx=50,cy=50,sw=12,circ=2*Math.PI*r;
  return(
    <svg width={size} height={size} viewBox="0 0 100 100">
      {data.map((d,i)=>{
        const pct=d.value/total,dash=pct*circ,gap=circ-dash,rot=cum;
        cum+=pct*360;
        return <circle key={i} cx={cx} cy={cy} r={r} fill="none" stroke={d.color} strokeWidth={sw} strokeDasharray={`${dash} ${gap}`} transform={`rotate(${rot} ${cx} ${cy})`} opacity="0.9"/>;
      })}
      <circle cx={cx} cy={cy} r={r-sw/2-2} fill={C.card}/>
    </svg>
  );
}

function MktSelector({mktFilter,setMktFilter,holdings}){
  const mkts=["ALL",...[...new Set(holdings.map(h=>h.mkt))]];
  const IDX={"US":"S&P 500","SG":"STI","CN":"HSI","JP":"Nikkei","EU":"CAC 40","GB":"FTSE","AU":"ASX"};
  return(
    <div style={{display:"flex",gap:5,overflowX:"auto",paddingBottom:4}}>
      {mkts.map(m=>{
        const cnt=m==="ALL"?holdings.length:holdings.filter(h=>h.mkt===m).length;
        const active=mktFilter===m;
        return(
          <button key={m} onClick={()=>setMktFilter(m)} style={{flexShrink:0,padding:"7px 11px",borderRadius:10,cursor:"pointer",background:active?C.accent:C.card,color:active?"#000":C.text,border:`1px solid ${active?C.accent:C.border}`,textAlign:"center",minWidth:64}}>
            <div style={{fontSize:12,fontWeight:800}}>{m==="ALL"?"ALL":m}</div>
            <div style={{fontSize:9,color:active?"#00000088":C.muted}}>{cnt} stocks</div>
            {m!=="ALL"&&<div style={{fontSize:8,color:active?"#00000066":C.muted+"88"}}>{IDX[m]||""}</div>}
          </button>
        );
      })}
    </div>
  );
}

// ─── Main App ─────────────────────────────────────────────────────────────────
function App(){
  const [tab,setTab]=useState("portfolio");
  const [holdings,setHoldings]=useState(ALL_H);
  const [trades,setTrades]=useState(ALL_T);
  const [sel,setSel]=useState(null);
  const [mktFilter,setMktFilter]=useState("ALL");
  const [chartPeriod,setChartPeriod]=useState("6m");
  const [detailPeriod,setDetailPeriod]=useState("6m");
  const [groupBy,setGroupBy]=useState("sector");
  const [search,setSearch]=useState("");
  const [tradeType,setTradeType]=useState("ALL");
  const [insightTab,setInsightTab]=useState("performers");
  const [aiText,setAiText]=useState({});
  const [aiLoad,setAiLoad]=useState({});
  const [showTradeForm,setShowTradeForm]=useState(false);
  const [editTradeId,setEditTradeId]=useState(null);
  const [tradeForm,setTradeForm]=useState({ticker:"",type:"BUY",date:new Date().toISOString().slice(0,10),price:"",shares:"",mkt:"US",ccy:"USD"});
  const [holdingEditId,setHoldingEditId]=useState(null);
  const [holdingForm,setHoldingForm]=useState({});
  const [deleteConfirm,setDeleteConfirm]=useState(null);
  const [refreshKey,setRefreshKey]=useState(0);
  const [lastRefresh,setLastRefresh]=useState(null);
  const [refreshAnim,setRefreshAnim]=useState(false);
  const [pendingChanges,setPendingChanges]=useState(0);

  // ── DB persistence ────────────────────────────────────────────────────────────
  const [dbStatus,setDbStatus]=useState('ready'); // 'ready' | 'saving' | 'saved' | 'error'
  const [isLoading,setIsLoading]=useState(true);
  const [priceStatus,setPriceStatus]=useState('idle'); // 'idle'|'fetching'|'done'|'error'
  const [priceUpdated,setPriceUpdated]=useState(null); // timestamp of last price update

  // ── Load data from Supabase on mount ─────────────────────────────────────────
  const [loadMsg,setLoadMsg]=useState('Connecting...');
  useEffect(()=>{
    if(!window.portfolioDB){setLoadMsg('ERROR: portfolioDB not found');setIsLoading(false);return;}
    setLoadMsg('Calling portfolioDB.load()...');
    window.portfolioDB.load().then(data=>{
      setLoadMsg('Got data: '+JSON.stringify({h:(data.holdings||[]).length,t:(data.trades||[]).length}));
      if(data.holdings&&data.holdings.length>0){
        setHoldings(data.holdings);
        setTrades(data.trades||[]);
        const fb={};
        (data.trades||[]).filter(t=>t.type==='BUY').forEach(t=>{
          if(!fb[t.ticker]||t.date<fb[t.ticker])fb[t.ticker]=t.date;
        });
        FIRST_BUY=fb;
        // Fetch live prices after data loads
        fetchLivePrices(data.holdings);
      } else {
        setLoadMsg('WARNING: holdings empty. data='+JSON.stringify(data).slice(0,200));
      }
      setIsLoading(false);
    }).catch(e=>{
      setLoadMsg('ERROR: '+e.message);
      setIsLoading(false);
    });
  },[]);

  // ── Live price updater ────────────────────────────────────────────────────────
  // Uses Claude AI (Anthropic API) to fetch prices — reliable, no CORS issues
  async function fetchLivePrices(currentHoldings) {
    if (!currentHoldings || currentHoldings.length === 0) return;
    setPriceStatus('fetching');

    const tickers = currentHoldings.map(h => h.ticker);
    const results = {};

    // Split into batches of 30 for the AI prompt
    const BATCH = 30;
    for (let i = 0; i < tickers.length; i += BATCH) {
      const batch = tickers.slice(i, i + BATCH);
      try {
        const prompt = `Return ONLY a JSON object (no markdown, no explanation) with the latest stock closing prices for these tickers: ${batch.join(', ')}
Use the most recent available closing price for each ticker.
Format: {"TICKER": price, "TICKER2": price2, ...}
Use numeric values only. If a ticker is unknown, omit it.`;

        const res = await fetch('https://api.anthropic.com/v1/messages', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            model: 'claude-sonnet-4-20250514',
            max_tokens: 1000,
            messages: [{ role: 'user', content: prompt }]
          })
        });
        const d = await res.json();
        const text = d.content?.map(c => c.text || '').join('').trim();
        const clean = text.replace(/```json|```/g, '').trim();
        const prices = JSON.parse(clean);
        Object.assign(results, prices);
      } catch(e) {
        console.warn('Price batch failed:', e.message);
      }
      if (i + BATCH < tickers.length) await new Promise(r => setTimeout(r, 200));
    }

    const successCount = Object.keys(results).length;
    console.log(`Prices fetched: ${successCount}/${tickers.length}`);
    console.log('Sample:', Object.entries(results).slice(0,5).map(([k,v])=>k+'='+v).join(', '));

    if (successCount === 0) { setPriceStatus('error'); return; }

    setHoldings(prev => {
      const updated = prev.map(h => {
        const newPrice = results[h.ticker];
        return newPrice && newPrice > 0 ? { ...h, price: parseFloat(newPrice.toFixed(4)) } : h;
      });
      if (window.portfolioDB) {
        window.portfolioDB.updateHoldings(updated).catch(e => console.warn('DB save failed:', e));
      }
      return updated;
    });

    setPriceUpdated(new Date());
    setPriceStatus('done');
    setRefreshKey(k => k + 1);
  }

  // Auto-persist holdings to SQLite whenever they change (debounced 600ms)
  useEffect(()=>{
    if(!window.portfolioDB)return;
    setDbStatus('saving');
    const timer=setTimeout(async()=>{
      try{
        await window.portfolioDB.updateHoldings(holdings);
        setDbStatus('saved');
        setTimeout(()=>setDbStatus('ready'),2000);
      }catch(e){
        console.error('DB save holdings failed:',e);
        setDbStatus('error');
      }
    },600);
    return()=>clearTimeout(timer);
  },[holdings]);

  // Auto-persist trades to SQLite whenever they change
  useEffect(()=>{
    if(!window.portfolioDB)return;
    // We save all trades in batch via updateHoldings approach or individually
    // For trades we use saveTrade for each new/edited trade (already called in submitTrade/deleteTrade)
    // This effect just marks status
  },[trades]);



  // Track mutations — increment pending count on any holdings/trades change
  const markDirty=()=>setPendingChanges(n=>n+1);

  function doRefresh(){
    setRefreshAnim(true);
    // Rebuild holdings from current trades — preserves manual metadata edits (price, sector etc.)
    const rebuilt=rebuildHoldingsFromTrades(trades, holdings);
    // Merge: trade-based shares/avgCost, but keep any manual price/intrinsic/sector edits
    const curMap={};
    holdings.forEach(h=>{curMap[h.ticker]=h;});
    const merged=rebuilt.map(h=>{
      const cur=curMap[h.ticker];
      if(!cur)return h;
      return{...cur,shares:h.shares,avgCost:h.avgCost};
    });
    setHoldings(merged);
    setRefreshKey(k=>k+1);
    setLastRefresh(new Date());
    setPendingChanges(0);
    setTimeout(()=>setRefreshAnim(false),800);
  }

  // Currencies separate from exchange/market
  const CCY={USD:{symbol:"$",r:1.36},SGD:{symbol:"S$",r:1.0},HKD:{symbol:"HK$",r:0.17},JPY:{symbol:"¥",r:0.0087},EUR:{symbol:"€",r:1.60},GBP:{symbol:"£",r:1.69},AUD:{symbol:"A$",r:0.87},CNY:{symbol:"¥",r:0.19},TWD:{symbol:"NT$",r:0.042}};
  const ccySymbol=ccy=>(CCY[ccy]?.symbol||"$");
  const ccyToSGD=(v,ccy)=>v*(CCY[ccy]?.r??1.36);

  // Ticker → Name lookup from existing holdings
  const tickerNames=useMemo(()=>{
    const map={};
    holdings.forEach(h=>{map[h.ticker]=h.name;});
    return map;
  },[holdings]);

  // Ticker validation / suggestion state
  const [tickerCheck,setTickerCheck]=useState({status:"idle",message:"",suggestions:[]});
  const [tickerSearchTerm,setTickerSearchTerm]=useState("");

  // Portfolio maths — all depend on holdings/trades/refreshKey so they recompute on refresh
  const totalValSGD=useMemo(()=>holdings.reduce((s,h)=>s+toSGD(h.price*h.shares,h.mkt),0),[holdings,refreshKey]);
  const totalCostSGD=useMemo(()=>holdings.reduce((s,h)=>s+toSGD(h.avgCost*h.shares,h.mkt),0),[holdings,refreshKey]);
  const unrealSGD=totalValSGD-totalCostSGD;
  const unrealPct=totalCostSGD?(unrealSGD/totalCostSGD)*100:0;
  const totalDivSGD=useMemo(()=>holdings.reduce((s,h)=>s+toSGD((h.divYield/100)*h.price*h.shares,h.mkt),0),[holdings,refreshKey]);
  const totalShares=useMemo(()=>holdings.reduce((s,h)=>s+h.shares,0),[holdings,refreshKey]);
  const avgCostSGD=totalShares?totalCostSGD/totalShares:0;
  const realizedSGD=useMemo(()=>trades.filter(t=>t.type==="SELL").reduce((s,t)=>s+toSGD(t.profit||0,t.mkt),0),[trades,refreshKey]);
  const wt=h=>filteredTotalSGD?(toSGD(h.price*h.shares,h.mkt)/filteredTotalSGD)*100:0;
  const wtTotal=h=>totalValSGD?(toSGD(h.price*h.shares,h.mkt)/totalValSGD)*100:0;

  const sectorData=useMemo(()=>{
    const subset=mktFilter==="ALL"?holdings:holdings.filter(h=>h.mkt===mktFilter);
    return SECTORS.map((sec,i)=>({label:sec,color:SCOL[i],value:subset.filter(h=>h.sector===sec).reduce((t,h)=>t+toSGD(h.price*h.shares,h.mkt),0)})).filter(d=>d.value>0);
  },[mktFilter,holdings,refreshKey]);
  const countryData=useMemo(()=>{
    const subset=mktFilter==="ALL"?holdings:holdings.filter(h=>h.mkt===mktFilter);
    return [...new Set(subset.map(h=>h.mkt))].map((m,i)=>({label:m,color:[C.accent,C.green,C.gold,C.purple,C.red,"#FF8C42","#62D2E8"][i%7],value:subset.filter(h=>h.mkt===m).reduce((s,h)=>s+toSGD(h.price*h.shares,h.mkt),0)}));
  },[mktFilter,holdings,refreshKey]);
  const filteredTotalSGD=useMemo(()=>{
    const subset=mktFilter==="ALL"?holdings:holdings.filter(h=>h.mkt===mktFilter);
    return subset.reduce((s,h)=>s+toSGD(h.price*h.shares,h.mkt),0);
  },[mktFilter,holdings,refreshKey]);

  const filtered=useMemo(()=>{
    let h=mktFilter==="ALL"?holdings:holdings.filter(x=>x.mkt===mktFilter);
    if(search)h=h.filter(x=>x.ticker.toLowerCase().includes(search.toLowerCase())||x.name.toLowerCase().includes(search.toLowerCase()));
    return h;
  },[mktFilter,search,holdings,refreshKey]);

  // Sorted performers
  const byGain=useMemo(()=>[...holdings].sort((a,b)=>((b.price-b.avgCost)/b.avgCost)-((a.price-a.avgCost)/a.avgCost)),[holdings,refreshKey]);
  const top10=byGain.slice(0,10);
  const worst10=[...byGain].reverse().slice(0,10);
  const buffettList=useMemo(()=>[...holdings].map(h=>({...h,...buffettScore(h)})).sort((a,b)=>b.score-a.score),[holdings,refreshKey]);

  // AI analysis
  async function analyse(h){
    if(aiText[h.ticker])return;
    setAiLoad(p=>({...p,[h.ticker]:true}));
    const sc=scoreH(h),m=MKT[h.mkt]||MKT.US,bs=buffettScore(h);
    const up=((h.intrinsic-h.price)/h.price*100).toFixed(1);
    const prompt=[
      "Buffett-style analysis for Singapore investor. 3-4 paragraphs.",
      "Stock: "+h.name+" ("+h.ticker+") Market: "+h.mkt+" "+m.code,
      "Price: "+m.symbol+h.price+" approx S$"+fmt(toSGD(h.price,h.mkt))+" Avg Cost: "+m.symbol+h.avgCost,
      "Intrinsic: "+m.symbol+h.intrinsic+" Upside: "+up+"%",
      "Moat: "+h.moat+" PE: "+h.peRatio+" Div: "+h.divYield+"%",
      "Buffett Score: "+bs.score+"/100 Action: "+bs.action,
      "Benchmark: "+m.index+" YTD "+m.idxYtd+"%",
      "1-Business quality and moat 2-Valuation 3-Risks 4-Buffett-style recommendation"
    ].join("\n");
    try{
      const res=await fetch("https://api.anthropic.com/v1/messages",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({model:"claude-sonnet-4-20250514",max_tokens:900,messages:[{role:"user",content:prompt}]})});
      const d=await res.json();
      setAiText(p=>({...p,[h.ticker]:d.content?.map(c=>c.text||"").join("")||"Unavailable."}));
    }catch(e){setAiText(p=>({...p,[h.ticker]:"AI analysis temporarily unavailable."}));}
    setAiLoad(p=>({...p,[h.ticker]:false}));
  }

  // Rebuild holdings from scratch based on ALL_H baseline + all BUY trades applied
  // This ensures any trade edit/delete correctly recalculates shares and avgCost
  function rebuildHoldingsFromTrades(tradeList, currentHoldings){
    const curH=currentHoldings||holdings;
    // Start from the original static holdings as a reference for metadata (name, sector, price, etc.)
    const meta={};
    ALL_H.forEach(h=>{meta[h.ticker]={...h};});
    // Also carry over any current holdings metadata for manually added holdings
    curH.forEach(h=>{if(!meta[h.ticker])meta[h.ticker]={...h};});

    // Group all BUY trades by ticker
    const buyMap={};
    tradeList.filter(t=>t.type==="BUY").forEach(t=>{
      if(!buyMap[t.ticker])buyMap[t.ticker]=[];
      buyMap[t.ticker].push(t);
    });
    // Group all SELL trades to subtract shares
    const sellMap={};
    tradeList.filter(t=>t.type==="SELL").forEach(t=>{
      if(!sellMap[t.ticker])sellMap[t.ticker]=[];
      sellMap[t.ticker].push(t);
    });

    // Tickers that appear in any trade
    const allTickers=new Set([...Object.keys(buyMap),...Object.keys(sellMap)]);
    // Also include tickers from original holdings that have NO trades at all
    ALL_H.forEach(h=>allTickers.add(h.ticker));

    const rebuilt=[];
    allTickers.forEach(ticker=>{
      const buys=buyMap[ticker]||[];
      const sells=sellMap[ticker]||[];
      const baseH=meta[ticker];

      // Calculate weighted average cost from buys
      let totalBuyShares=0,totalBuyCost=0;
      buys.forEach(b=>{totalBuyShares+=b.shares;totalBuyCost+=b.shares*b.price;});
      const totalSellShares=sells.reduce((s,t)=>s+t.shares,0);
      const netShares=totalBuyShares-totalSellShares;

      if(netShares<=0)return; // fully sold, skip

      const computedAvgCost=totalBuyShares>0?totalBuyCost/totalBuyShares:baseH?.avgCost||0;

      // If we have base metadata, use it; otherwise create minimal entry
      if(baseH){
        rebuilt.push({
          ...baseH,
          shares:netShares,
          avgCost:parseFloat(computedAvgCost.toFixed(4)),
        });
      } else {
        // New ticker added via trades with no base metadata
        rebuilt.push({
          id:Date.now()+Math.random(),ticker,name:ticker,mkt:"US",
          sector:"Technology",msStyle:"Large Blend",
          shares:netShares,avgCost:parseFloat(computedAvgCost.toFixed(4)),
          price:computedAvgCost,intrinsic:computedAvgCost*1.1,
          moat:"Narrow",divYield:0,senateBuys:0,senateSells:0,peRatio:20,revenueGrowth:0
        });
      }
    });
    return rebuilt;
  }

  function submitTrade(){
    const {ticker,type,date,price,shares,mkt,ccy}=tradeForm;
    if(!ticker||!price||!shares||!date)return;
    const p=parseFloat(price),s=parseInt(shares),tU=ticker.toUpperCase().trim();
    if(isNaN(p)||isNaN(s)||s<=0||p<=0)return;

    let newTrades;
    if(editTradeId!=null){
      newTrades=trades.map(t=>t.id===editTradeId?{...t,ticker:tU,type,date,price:p,shares:s,mkt,ccy}:t);
      setEditTradeId(null);
    } else {
      const newTrade={id:Date.now(),ticker:tU,type,date,price:p,shares:s,mkt,ccy,profit:type==="SELL"?0:undefined};
      newTrades=[newTrade,...trades];
    }

    setTrades(newTrades);
    const rebuiltH=rebuildHoldingsFromTrades(newTrades, holdings);
    setHoldings(rebuiltH);
    if(window.portfolioDB){window.portfolioDB.updateHoldings(rebuiltH).catch(e=>console.error('DB:',e));}
    setShowTradeForm(false);
    setTradeForm({ticker:"",type:"BUY",date:new Date().toISOString().slice(0,10),price:"",shares:"",mkt:"US",ccy:"USD"});
    markDirty();
  }

  function deleteTrade(id){
    const newTrades=trades.filter(t=>t.id!==id);
    setTrades(newTrades);
    const rebuiltH2=rebuildHoldingsFromTrades(newTrades, holdings);
    setHoldings(rebuiltH2);
    if(window.portfolioDB){window.portfolioDB.deleteTrade(id).catch(e=>console.error('DB:',e));window.portfolioDB.updateHoldings(rebuiltH2).catch(e=>console.error('DB:',e));}
    markDirty();
  }

  function startEditTrade(t){
    setTradeForm({ticker:t.ticker,type:t.type,date:t.date,price:String(t.price),shares:String(t.shares),mkt:t.mkt||"US",ccy:t.ccy||"USD"});
    setEditTradeId(t.id);
    setShowTradeForm(true);
    setTickerCheck({status:"idle",message:"",suggestions:[]});
  }

  async function lookupTicker(query){
    if(!query||query.trim().length<2)return;
    setTickerCheck({status:"loading",message:"Searching...",suggestions:[]});
    const term=query.trim();
    // First check if it's already in portfolio
    const inPort=holdings.find(h=>h.ticker===term.toUpperCase());
    if(inPort){
      setTickerCheck({status:"found",message:inPort.name,suggestions:[],confirmed:inPort.ticker});
      setTradeForm(f=>({...f,ticker:inPort.ticker}));
      return;
    }
    // Use AI to look up the ticker
    const prompt="You are a financial ticker symbol lookup. Given a company name or ticker, return ONLY a JSON object with no markdown, no explanation.\nQuery: "+term+"\nReturn: {\"found\":true/false,\"ticker\":\"TICKER\",\"name\":\"Full Company Name\",\"exchange\":\"US/SG/CN/JP/EU/HK\",\"suggestions\":[{\"ticker\":\"T1\",\"name\":\"Name1\"},{\"ticker\":\"T2\",\"name\":\"Name2\"}]}\nIf the query looks like a ticker symbol, validate it and return its full name. If it looks like a company name, suggest up to 3 matching tickers. Always populate suggestions array with 1-3 options.";
    try{
      const res=await fetch("https://api.anthropic.com/v1/messages",{
        method:"POST",headers:{"Content-Type":"application/json"},
        body:JSON.stringify({model:"claude-sonnet-4-20250514",max_tokens:300,messages:[{role:"user",content:prompt}]})
      });
      const d=await res.json();
      const text=d.content?.map(c=>c.text||"").join("").trim();
      // Strip possible markdown fences
      const clean=text.replace(/^```json\s*/,"").replace(/```\s*$/,"").trim();
      const parsed=JSON.parse(clean);
      if(parsed.found){
        setTickerCheck({status:"found",message:parsed.name,suggestions:parsed.suggestions||[],confirmed:parsed.ticker});
        if(parsed.ticker)setTradeForm(f=>({...f,ticker:parsed.ticker}));
      } else {
        setTickerCheck({status:"suggestions",message:"Not found. Did you mean:",suggestions:parsed.suggestions||[]});
      }
    }catch(e){
      setTickerCheck({status:"error",message:"Lookup failed. Enter ticker manually.",suggestions:[]});
    }
  }

  function openEditHolding(h){
    setHoldingForm({
      ticker:h.ticker,name:h.name||"",mkt:h.mkt,sector:h.sector||"Technology",
      shares:String(h.shares),avgCost:String(h.avgCost),price:String(h.price),
      intrinsic:String(h.intrinsic||""),divYield:String(h.divYield||0),
      peRatio:String(h.peRatio||0),moat:h.moat||"Narrow",msStyle:h.msStyle||"Large Blend"
    });
    setHoldingEditId(h.id);
    setSel(null);
  }

  function saveHolding(){
    const f=holdingForm;
    const s=parseInt(f.shares),ac=parseFloat(f.avgCost),pr=parseFloat(f.price);
    if(!f.ticker||isNaN(s)||isNaN(ac)||isNaN(pr)||s<=0)return;
    setHoldings(prev=>prev.map(h=>
      h.id===holdingEditId
        ?{...h,ticker:f.ticker.trim().toUpperCase(),name:f.name||f.ticker,mkt:f.mkt,sector:f.sector,
           shares:s,avgCost:ac,price:pr,intrinsic:parseFloat(f.intrinsic)||pr*1.1,
           divYield:parseFloat(f.divYield)||0,peRatio:parseFloat(f.peRatio)||0,
           moat:f.moat,msStyle:f.msStyle}
        :h
    ));
    setHoldingEditId(null);
    setHoldingForm({});
    if(window.portfolioDB){window.portfolioDB.updateHoldings(holdings).catch(e=>console.error('DB:',e));}
    markDirty();
  }

  function confirmDeleteHolding(id){setDeleteConfirm(id);}
  function doDeleteHolding(){
    const ticker=holdings.find(h=>h.id===deleteConfirm)?.ticker;
    setHoldings(prev=>prev.filter(h=>h.id!==deleteConfirm));
    if(window.portfolioDB&&ticker){window.portfolioDB.deleteHolding(ticker).catch(e=>console.error('DB:',e));}
    setDeleteConfirm(null);
    setSel(null);
    markDirty();
  }

  const card={background:C.card,borderRadius:12,padding:14,marginBottom:10,border:`1px solid ${C.border}`};
  const cardT={fontSize:11,fontWeight:700,color:C.muted,textTransform:"uppercase",letterSpacing:"0.08em",marginBottom:10};
  const row={display:"flex",justifyContent:"space-between",alignItems:"center"};
  const pill=a=>({padding:"5px 11px",borderRadius:20,fontSize:11,fontWeight:a?700:500,background:a?C.accent:"transparent",color:a?C.bg:C.muted,border:`1px solid ${a?C.accent:C.border}`,cursor:"pointer"});
  const smPill=a=>({padding:"3px 8px",borderRadius:14,fontSize:10,fontWeight:a?700:500,background:a?C.surface:C.bg,color:a?C.accent:C.muted,border:`1px solid ${a?C.accent:C.border}`,cursor:"pointer"});
  const inp={width:"100%",background:C.surface,border:`1px solid ${C.border}`,borderRadius:8,padding:"9px 12px",color:C.text,fontSize:13,outline:"none",boxSizing:"border-box"};
  const modal={position:"fixed",inset:0,background:"rgba(0,0,0,0.82)",display:"flex",alignItems:"flex-end",zIndex:50};
  const mCard={background:C.card,borderRadius:"20px 20px 0 0",padding:20,width:"100%",maxWidth:430,margin:"0 auto",maxHeight:"92vh",overflowY:"auto"};
  const sbox=col=>({background:C.surface,borderRadius:10,padding:"10px 12px",border:`1px solid ${col?col+"35":C.border}`});
  const PERIODS=["30d","6m","1y","5y","all"];
  const PLBL={"30d":"30D","6m":"6M","1y":"1Y","5y":"5Y","all":"All"};

  // ── Portfolio tab ────────────────────────────────────────────────────────────
  function PortfolioView(){
    const activeM=MKT[mktFilter];
    return(
      <>
        <div style={{marginBottom:12}}>
          <div style={cardT}>Select Market</div>
          <MktSelector mktFilter={mktFilter} setMktFilter={setMktFilter} holdings={holdings}/>
        </div>
        <div style={card}>
          <div style={{...row,marginBottom:10}}>
            <div style={cardT}>{mktFilter==="ALL"?"All vs S&P 500":`${mktFilter} vs ${activeM?.index}`}</div>
            <div style={{display:"flex",gap:3}}>
              {PERIODS.map(p=><button key={p} style={smPill(chartPeriod===p)} onClick={()=>setChartPeriod(p)}>{PLBL[p]}</button>)}
            </div>
          </div>
          <PerfChart mktFilter={mktFilter} period={chartPeriod}/>
          {mktFilter!=="ALL"&&activeM&&(
            <div style={{marginTop:8,padding:"5px 10px",background:C.surface,borderRadius:6,fontSize:10,display:"flex",gap:10,flexWrap:"wrap"}}>
              <span style={{color:C.muted}}>Index: <b style={{color:C.text}}>{activeM.index}</b></span>
              <span style={{color:activeM.idxChange>=0?C.green:C.red}}>{activeM.idxChange>=0?"+":""}{activeM.idxChange}% today</span>
              <span style={{color:activeM.idxYtd>=0?C.green:C.red}}>YTD {activeM.idxYtd>=0?"+":""}{activeM.idxYtd}%</span>
            </div>
          )}
        </div>
        <div style={card}>
          <div style={cardT}>Allocation (SGD)</div>
          <div style={{display:"flex",gap:12,alignItems:"center"}}>
            <DonutChart data={groupBy==="sector"?sectorData:countryData}/>
            <div style={{flex:1}}>
              <div style={{display:"flex",gap:5,marginBottom:8}}>
                <button style={pill(groupBy==="sector")} onClick={()=>setGroupBy("sector")}>Sector</button>
                <button style={pill(groupBy==="country")} onClick={()=>setGroupBy("country")}>Country</button>
              </div>
              {(groupBy==="sector"?sectorData:countryData).slice(0,8).map(d=>(
                <div key={d.label} style={{display:"flex",justifyContent:"space-between",marginBottom:3}}>
                  <div style={{display:"flex",alignItems:"center",gap:5}}><div style={{width:6,height:6,borderRadius:3,background:d.color}}/><span style={{fontSize:9,color:C.mutedLight}}>{d.label}</span></div>
                  <span style={{fontSize:9,fontWeight:700}}>{filteredTotalSGD>0?((d.value/filteredTotalSGD)*100).toFixed(1):0}%</span>
                </div>
              ))}
            </div>
          </div>
        </div>
        <input style={{...inp,marginBottom:10}} placeholder={`Search ${filtered.length} holdings...`} value={search} onChange={e=>setSearch(e.target.value)}/>
        <div style={{fontSize:11,fontWeight:700,color:C.muted,textTransform:"uppercase",letterSpacing:"0.08em",marginBottom:10}}>{filtered.length} Holdings{mktFilter!=="ALL"?` · ${mktFilter}`:""}</div>
        {filtered.map(h=>{
          const localVal=h.price*h.shares,localCost=h.avgCost*h.shares,localGain=localVal-localCost;
          const gainPct=((h.price-h.avgCost)/h.avgCost)*100;
          const upside=((h.intrinsic-h.price)/h.price)*100;
          const sgdVal=toSGD(localVal,h.mkt),sgdGain=toSGD(localGain,h.mkt);
          const w=wt(h),pos=gainPct>=0,sc=scoreH(h),r=getRec(h);
          const sCol=SCOL[SECTORS.indexOf(h.sector)%SCOL.length];
          return(
            <div key={h.id} style={{...card,cursor:"pointer"}} onClick={()=>{setSel(h);setDetailPeriod("6m");}}>
              <div style={{display:"flex",justifyContent:"space-between",alignItems:"flex-start",marginBottom:6}}>
                <div style={{flex:1,minWidth:0}}>
                  <div style={{display:"flex",alignItems:"center",gap:5,flexWrap:"wrap"}}>
                    <span style={{fontWeight:800,fontSize:14}}>{h.ticker}</span>
                    <Chip mkt={h.mkt}/>
                    <Tag col={sCol}>{h.sector}</Tag>
                  </div>
                  <div style={{fontSize:11,color:C.muted,marginTop:1,whiteSpace:"nowrap",overflow:"hidden",textOverflow:"ellipsis",maxWidth:200}}>{h.name}</div>
                  <div style={{fontSize:10,color:C.mutedLight,marginTop:3}}>
                    Avg Cost: <b style={{color:C.text}}>{fmtL(h.avgCost,h.mkt)}</b>
                    <span style={{color:C.muted,fontWeight:400}}> ({fmtS(toSGD(h.avgCost,h.mkt))})</span>
                  </div>
                  <div style={{fontSize:10,color:C.mutedLight,marginTop:1}}>
                    Intrinsic: <b style={{color:upside>=0?C.green:C.red}}>{fmtL(h.intrinsic,h.mkt)}</b>
                    <span style={{color:C.muted,fontWeight:400}}> {upside>=0?"+":""}{fmt(upside,1)}% upside</span>
                  </div>
                </div>
                <div style={{display:"flex",flexDirection:"column",alignItems:"flex-end",gap:4,flexShrink:0,marginLeft:8}}>
                  <div style={{textAlign:"right"}}>
                    <div style={{fontSize:14,fontWeight:800}}>{fmtL(h.price,h.mkt)}</div>
                    <div style={{fontSize:9,color:C.muted}}>{fmtS(toSGD(h.price,h.mkt))}</div>
                    <div style={{fontSize:11,color:pos?C.green:C.red,fontWeight:700}}>{fmtPct(gainPct)}</div>
                    <div style={{fontSize:9,color:C.muted}}>{h.shares.toLocaleString()} sh</div>
                  </div>
                  <div style={{display:"flex",gap:4}} onClick={e=>e.stopPropagation()}>
                    <button onClick={()=>openEditHolding(h)} style={{fontSize:10,padding:"3px 8px",borderRadius:5,border:`1px solid ${C.border}`,background:"transparent",color:C.accent,cursor:"pointer",fontWeight:600}}>Edit</button>
                    <button onClick={()=>confirmDeleteHolding(h.id)} style={{fontSize:10,padding:"3px 8px",borderRadius:5,border:`1px solid ${C.red}44`,background:"transparent",color:C.red,cursor:"pointer",fontWeight:600}}>Del</button>
                  </div>
                </div>
              </div>
              <div style={{background:C.accent+"0D",border:`1px solid ${C.accentDim}20`,borderRadius:8,padding:"7px 10px",marginBottom:7}}>
                <div style={{display:"grid",gridTemplateColumns:"1fr 1fr 1fr",gap:4,marginBottom:4}}>
                  <div><div style={{fontSize:9,color:C.muted}}>Value</div><div style={{fontSize:11,fontWeight:800}}>{fmtL(localVal,h.mkt,0)}</div><div style={{fontSize:9,color:C.muted}}>{fmtS(sgdVal)}</div></div>
                  <div style={{textAlign:"center"}}><div style={{fontSize:9,color:C.muted}}>Weight{mktFilter!=="ALL"?` (${mktFilter})`:""}</div><div style={{fontSize:14,fontWeight:800,color:C.accent}}>{w.toFixed(1)}%</div></div>
                  <div style={{textAlign:"right"}}><div style={{fontSize:9,color:C.muted}}>Unr. P&amp;L</div><div style={{fontSize:11,fontWeight:800,color:pos?C.green:C.red}}>{pos?"+":"-"}{fmtL(Math.abs(localGain),h.mkt,0)}</div><div style={{fontSize:9,color:C.muted}}>{pos?"+":"-"}{fmtS(Math.abs(sgdGain))}</div></div>
                </div>
                <div style={{height:3,borderRadius:2,background:C.border}}><div style={{width:`${Math.min(w*2.5,100)}%`,height:"100%",borderRadius:2,background:C.accent,opacity:0.7}}/></div>
              </div>
              <div style={{display:"flex",justifyContent:"space-between",alignItems:"center"}}>
                <div style={{flex:1,marginRight:10}}><ScoreBar score={sc.all} max={10}/></div>
                <div style={{display:"flex",gap:4}}>
                  <Bdg label={h.moat+" Moat"} bg={h.moat==="Wide"?"#1A2E1A":"#2A2A1A"} color={h.moat==="Wide"?C.green:C.gold}/>
                  <Bdg label={r.lbl} bg={r.col+"22"} color={r.col}/>
                </div>
              </div>
            </div>
          );
        })}
      </>
    );
  }

  // ── Insights tab ─────────────────────────────────────────────────────────────
  function InsightsView(){
    return(
      <>
        <div style={{display:"flex",gap:5,marginBottom:14,overflowX:"auto"}}>
          {[["performers","Performers"],["senate","Senate"],["buffett","Buffett"]].map(([id,lbl])=>(
            <button key={id} style={{...pill(insightTab===id),whiteSpace:"nowrap",flexShrink:0}} onClick={()=>setInsightTab(id)}>{lbl}</button>
          ))}
        </div>

        {insightTab==="performers"&&(
          <>
            <div style={card}>
              <div style={cardT}>Top 10 Performers (% gain from avg cost)</div>
              {top10.map((h,i)=>{
                const g=((h.price-h.avgCost)/h.avgCost)*100;
                const lg=(h.price-h.avgCost)*h.shares;
                const up=((h.intrinsic-h.price)/h.price)*100;
                const hist=HIST[h.ticker]?.["6m"]||[];
                return(
                  <div key={h.ticker} style={{marginBottom:10,paddingBottom:10,borderBottom:i<9?`1px solid ${C.border}`:"none",cursor:"pointer"}} onClick={()=>{setSel(h);setDetailPeriod("6m");}}>
                    <div style={{display:"flex",alignItems:"center",gap:10}}>
                      <div style={{fontSize:16,fontWeight:800,color:C.green,width:26,textAlign:"center",flexShrink:0}}>{i+1}</div>
                      <div style={{flex:1,minWidth:0}}>
                        <div style={{display:"flex",alignItems:"center",gap:5}}><span style={{fontWeight:700,fontSize:13}}>{h.ticker}</span><Chip mkt={h.mkt}/></div>
                        <div style={{fontSize:10,color:C.muted,whiteSpace:"nowrap",overflow:"hidden",textOverflow:"ellipsis"}}>{h.name}</div>
                      </div>
                      <div style={{flexShrink:0}}><MiniSparkline data={hist} color={C.green}/></div>
                      <div style={{textAlign:"right",flexShrink:0}}>
                        <div style={{fontSize:14,fontWeight:800,color:C.green}}>{fmtPct(g)}</div>
                        <div style={{fontSize:10,fontWeight:700,color:C.green}}>+{fmtL(lg,h.mkt,0)}</div>
                      </div>
                    </div>
                    <div style={{display:"grid",gridTemplateColumns:"1fr 1fr 1fr",gap:4,marginTop:6,background:C.surface,borderRadius:6,padding:"5px 8px"}}>
                      <div><div style={{fontSize:8,color:C.muted}}>Price</div><div style={{fontSize:11,fontWeight:700}}>{fmtL(h.price,h.mkt)}</div></div>
                      <div><div style={{fontSize:8,color:C.muted}}>Avg Cost</div><div style={{fontSize:11,fontWeight:700,color:C.mutedLight}}>{fmtL(h.avgCost,h.mkt)}</div></div>
                      <div style={{textAlign:"right"}}><div style={{fontSize:8,color:C.muted}}>Intrinsic</div><div style={{fontSize:11,fontWeight:700,color:up>=0?C.green:C.red}}>{fmtL(h.intrinsic,h.mkt)}</div></div>
                    </div>
                  </div>
                );
              })}
            </div>
            <div style={card}>
              <div style={cardT}>Top 10 Worst Performers</div>
              {worst10.map((h,i)=>{
                const g=((h.price-h.avgCost)/h.avgCost)*100;
                const lg=(h.price-h.avgCost)*h.shares;
                const pos=lg>=0;
                const up=((h.intrinsic-h.price)/h.price)*100;
                const hist=HIST[h.ticker]?.["6m"]||[];
                return(
                  <div key={h.ticker} style={{marginBottom:10,paddingBottom:10,borderBottom:i<9?`1px solid ${C.border}`:"none",cursor:"pointer"}} onClick={()=>{setSel(h);setDetailPeriod("6m");}}>
                    <div style={{display:"flex",alignItems:"center",gap:10}}>
                      <div style={{fontSize:16,fontWeight:800,color:C.red,width:26,textAlign:"center",flexShrink:0}}>{i+1}</div>
                      <div style={{flex:1,minWidth:0}}>
                        <div style={{display:"flex",alignItems:"center",gap:5}}><span style={{fontWeight:700,fontSize:13}}>{h.ticker}</span><Chip mkt={h.mkt}/></div>
                        <div style={{fontSize:10,color:C.muted,whiteSpace:"nowrap",overflow:"hidden",textOverflow:"ellipsis"}}>{h.name}</div>
                      </div>
                      <div style={{flexShrink:0}}><MiniSparkline data={hist} color={C.red}/></div>
                      <div style={{textAlign:"right",flexShrink:0}}>
                        <div style={{fontSize:14,fontWeight:800,color:C.red}}>{fmtPct(g)}</div>
                        <div style={{fontSize:10,fontWeight:700,color:pos?C.green:C.red}}>{pos?"+":"-"}{fmtL(Math.abs(lg),h.mkt,0)}</div>
                      </div>
                    </div>
                    <div style={{display:"grid",gridTemplateColumns:"1fr 1fr 1fr",gap:4,marginTop:6,background:C.surface,borderRadius:6,padding:"5px 8px"}}>
                      <div><div style={{fontSize:8,color:C.muted}}>Price</div><div style={{fontSize:11,fontWeight:700}}>{fmtL(h.price,h.mkt)}</div></div>
                      <div><div style={{fontSize:8,color:C.muted}}>Avg Cost</div><div style={{fontSize:11,fontWeight:700,color:C.mutedLight}}>{fmtL(h.avgCost,h.mkt)}</div></div>
                      <div style={{textAlign:"right"}}><div style={{fontSize:8,color:C.muted}}>Intrinsic</div><div style={{fontSize:11,fontWeight:700,color:up>=0?C.green:C.red}}>{fmtL(h.intrinsic,h.mkt)}</div></div>
                    </div>
                  </div>
                );
              })}
            </div>
          </>
        )}

        {insightTab==="senate"&&(
          <div style={card}>
            <div style={cardT}>US Senate Top 10 Recent Trades</div>
            <div style={{fontSize:10,color:C.muted,marginBottom:12,padding:"6px 10px",background:C.surface,borderRadius:6}}>
              Prices sourced from Unusual Whales &amp; Quiver Quantitative. Est. trade price based on date-of-filing midpoint. STOCK Act requires disclosure within 30-45 days.
            </div>
            {SENATE.map((s,i)=>{
              const inPort=holdings.find(h=>h.ticker===s.ticker);
              const sinceGain=s.estPrice>0?((s.priceNow-s.estPrice)/s.estPrice*100):null;
              const isProfit=s.action==="BUY"?sinceGain>=0:sinceGain<=0;
              return(
                <div key={i} style={{marginBottom:14,paddingBottom:14,borderBottom:i<SENATE.length-1?`1px solid ${C.border}`:"none"}}>
                  <div style={{...row,marginBottom:6}}>
                    <div>
                      <div style={{display:"flex",alignItems:"center",gap:6,marginBottom:2}}>
                        <span style={{fontWeight:700,fontSize:13}}>{s.name}</span>
                        <Bdg label={s.party} bg={s.party==="D"?"#1e3a5f":"#3d1515"} color={s.party==="D"?"#60a5fa":"#f87171"}/>
                      </div>
                      <div style={{fontSize:10,color:C.muted}}>{s.date} · {s.sector}</div>
                      <div style={{fontSize:9,color:C.mutedLight,marginTop:1}}>via {s.source}</div>
                    </div>
                    <div style={{textAlign:"right"}}>
                      <div style={{display:"flex",alignItems:"center",gap:5,justifyContent:"flex-end",marginBottom:2}}>
                        <span style={{fontWeight:800,fontSize:14}}>{s.ticker}</span>
                        <Bdg label={s.action} bg={s.action==="BUY"?C.green+"22":C.red+"22"} color={s.action==="BUY"?C.green:C.red}/>
                      </div>
                      <div style={{fontSize:10,color:C.gold,fontWeight:600}}>{s.amount}</div>
                    </div>
                  </div>
                  {/* Price strip */}
                  <div style={{display:"grid",gridTemplateColumns:"1fr 1fr 1fr",gap:4,background:C.surface,borderRadius:7,padding:"7px 10px",marginBottom:inPort?6:0}}>
                    <div>
                      <div style={{fontSize:8,color:C.muted}}>Est. Trade Price</div>
                      <div style={{fontSize:12,fontWeight:700,color:C.gold}}>${fmt(s.estPrice)}</div>
                      <div style={{fontSize:8,color:C.muted}}>USD (est.)</div>
                    </div>
                    <div style={{textAlign:"center"}}>
                      <div style={{fontSize:8,color:C.muted}}>Current Price</div>
                      <div style={{fontSize:12,fontWeight:700}}>${fmt(s.priceNow)}</div>
                      <div style={{fontSize:8,color:C.muted}}>as of Apr 2026</div>
                    </div>
                    <div style={{textAlign:"right"}}>
                      <div style={{fontSize:8,color:C.muted}}>Since Trade</div>
                      <div style={{fontSize:12,fontWeight:700,color:isProfit?C.green:C.red}}>
                        {sinceGain!=null?(sinceGain>=0?"+":"")+fmt(sinceGain,1)+"%":"--"}
                      </div>
                      <div style={{fontSize:8,color:isProfit?C.green:C.red}}>{isProfit?"Profitable":"Losing"}</div>
                    </div>
                  </div>
                  {inPort&&(
                    <div style={{background:C.accent+"0D",borderRadius:6,padding:"6px 8px",fontSize:10}}>
                      <div style={{display:"flex",gap:10,alignItems:"center",flexWrap:"wrap",marginBottom:4}}>
                        <span style={{color:C.accent,fontWeight:700}}>In your portfolio</span>
                        <span style={{color:((inPort.price-inPort.avgCost)/inPort.avgCost*100)>=0?C.green:C.red,fontWeight:700}}>{fmtPct((inPort.price-inPort.avgCost)/inPort.avgCost*100)}</span>
                      </div>
                      <div style={{display:"grid",gridTemplateColumns:"1fr 1fr 1fr",gap:4}}>
                        <div><div style={{fontSize:8,color:C.muted}}>Price</div><div style={{fontSize:11,fontWeight:700}}>{fmtL(inPort.price,inPort.mkt)}</div></div>
                        <div><div style={{fontSize:8,color:C.muted}}>Avg Cost</div><div style={{fontSize:11,fontWeight:700,color:C.mutedLight}}>{fmtL(inPort.avgCost,inPort.mkt)}</div></div>
                        <div style={{textAlign:"right"}}><div style={{fontSize:8,color:C.muted}}>Intrinsic</div><div style={{fontSize:11,fontWeight:700,color:((inPort.intrinsic-inPort.price)/inPort.price*100)>=0?C.green:C.red}}>{fmtL(inPort.intrinsic,inPort.mkt)}</div></div>
                      </div>
                    </div>
                  )}
                </div>
              );
            })}
          </div>
        )}

        {insightTab==="buffett"&&(
          <>
            <div style={{...card,background:"#1A1200",border:`1px solid ${C.gold}30`}}>
              <div style={{fontSize:10,color:C.gold,lineHeight:1.6}}>
                <b>"Price is what you pay. Value is what you get."</b><br/>
                Score: Moat 30pts + Dividend 20pts + Upside 25pts + Fair PE 15pts + Gain track 10pts
              </div>
            </div>
            {[
              {filter:(h)=>h.action==="BUY MORE"||h.action==="ADD GRADUALLY",title:"Buy More (Score 65+)",emptyMsg:"No strong buys at current prices"},
              {filter:(h)=>h.action==="HOLD",title:"Hold (Score 35-64)",emptyMsg:"No holds identified"},
              {filter:(h)=>h.action==="CONSIDER SELLING"||h.action==="WATCH",title:"Consider Selling / Watch",emptyMsg:"No sells identified"},
            ].map(({filter:fn,title,emptyMsg})=>{
              const list=buffettList.filter(fn).slice(0,8);
              return(
                <div key={title} style={card}>
                  <div style={cardT}>{title}</div>
                  {list.length===0&&<div style={{fontSize:12,color:C.muted,padding:"8px 0"}}>{emptyMsg}</div>}
                  {list.map((h,i)=>{
                    const g=((h.price-h.avgCost)/h.avgCost)*100;
                    const up=((h.intrinsic-h.price)/h.price)*100;
                    return(
                      <div key={h.ticker} style={{marginBottom:11,paddingBottom:11,borderBottom:i<list.length-1?`1px solid ${C.border}`:"none",cursor:"pointer"}} onClick={()=>{setSel(h);setDetailPeriod("6m");}}>
                        <div style={{display:"flex",alignItems:"center",gap:10}}>
                          <div style={{width:36,height:36,borderRadius:8,background:h.col+"22",display:"flex",alignItems:"center",justifyContent:"center",flexShrink:0}}>
                            <span style={{fontSize:12,fontWeight:800,color:h.col}}>{h.score}</span>
                          </div>
                          <div style={{flex:1,minWidth:0}}>
                            <div style={{display:"flex",alignItems:"center",gap:5}}>
                              <span style={{fontWeight:700}}>{h.ticker}</span>
                              <Chip mkt={h.mkt}/>
                              <Bdg label={h.action} bg={h.col+"22"} color={h.col}/>
                            </div>
                            <div style={{fontSize:10,color:C.muted,marginTop:1}}>{h.reason}</div>
                          </div>
                          <div style={{textAlign:"right",flexShrink:0}}>
                            <div style={{fontSize:12,fontWeight:700,color:g>=0?C.green:C.red}}>{fmtPct(g)}</div>
                          </div>
                        </div>
                        <div style={{display:"grid",gridTemplateColumns:"1fr 1fr 1fr",gap:4,marginTop:6,background:C.surface,borderRadius:6,padding:"5px 8px"}}>
                          <div><div style={{fontSize:8,color:C.muted}}>Price</div><div style={{fontSize:11,fontWeight:700}}>{fmtL(h.price,h.mkt)}</div></div>
                          <div><div style={{fontSize:8,color:C.muted}}>Avg Cost</div><div style={{fontSize:11,fontWeight:700,color:C.mutedLight}}>{fmtL(h.avgCost,h.mkt)}</div></div>
                          <div style={{textAlign:"right"}}><div style={{fontSize:8,color:C.muted}}>Intrinsic</div><div style={{fontSize:11,fontWeight:700,color:up>=0?C.green:C.red}}>{fmtL(h.intrinsic,h.mkt)}</div></div>
                        </div>
                      </div>
                    );
                  })}
                </div>
              );
            })}
          </>
        )}
      </>
    );
  }

  // ── Indices tab ──────────────────────────────────────────────────────────────
  function IndexView(){
    const mktsInPort=[...new Set(holdings.map(h=>h.mkt))];
    return(
      <>
        <div style={{...cardT,paddingLeft:0}}>Your Markets vs Benchmarks</div>
        {mktsInPort.map(mkt=>{
          const m=MKT[mkt]||MKT.US;
          const cnt=holdings.filter(h=>h.mkt===mkt).length;
          const portCost=holdings.filter(h=>h.mkt===mkt).reduce((s,h)=>s+toSGD(h.avgCost*h.shares,h.mkt),0);
          const portVal=holdings.filter(h=>h.mkt===mkt).reduce((s,h)=>s+toSGD(h.price*h.shares,h.mkt),0);
          const portPct=portCost?(portVal-portCost)/portCost*100:0;
          const beat=portPct>m.idxYtd;
          return(
            <div key={mkt} style={{...card,borderLeft:`3px solid ${beat?C.green:C.mutedLight}`}}>
              <div style={{...row,marginBottom:8}}>
                <div>
                  <div style={{fontWeight:700,fontSize:14,display:"flex",alignItems:"center",gap:6,marginBottom:4}}>{m.index}<Chip mkt={mkt}/></div>
                  <div style={{display:"flex",gap:5}}>
                    <Tag col={m.idxYtd>=0?C.green:C.red}>Index YTD {m.idxYtd>=0?"+":""}{m.idxYtd}%</Tag>
                    <Tag col={beat?C.green:C.red}>{beat?"Outperforming":"Underperforming"}</Tag>
                  </div>
                </div>
                <div style={{textAlign:"right"}}>
                  <div style={{fontSize:15,fontWeight:800}}>{m.symbol}{fmt(m.idxVal,1)}</div>
                  <div style={{fontSize:11,color:m.idxChange>=0?C.green:C.red,fontWeight:600}}>{m.idxChange>=0?"+":""}{m.idxChange}% today</div>
                </div>
              </div>
              <div style={{background:C.surface,borderRadius:8,padding:"8px 10px"}}>
                <div style={{display:"flex",gap:4,alignItems:"center",marginBottom:4}}>
                  <span style={{fontSize:9,color:C.muted,width:48}}>Portfolio</span>
                  <div style={{flex:1,height:5,borderRadius:2,background:C.border,overflow:"hidden"}}><div style={{width:`${Math.min(Math.abs(portPct)/35*100,100)}%`,height:"100%",background:portPct>=0?C.green:C.red,borderRadius:2}}/></div>
                  <span style={{fontSize:9,fontWeight:700,color:portPct>=0?C.green:C.red,width:40,textAlign:"right"}}>{portPct>=0?"+":""}{fmt(portPct,1)}%</span>
                </div>
                <div style={{display:"flex",gap:4,alignItems:"center",marginBottom:6}}>
                  <span style={{fontSize:9,color:C.muted,width:48}}>Index</span>
                  <div style={{flex:1,height:5,borderRadius:2,background:C.border,overflow:"hidden"}}><div style={{width:`${Math.min(Math.abs(m.idxYtd)/35*100,100)}%`,height:"100%",background:m.idxYtd>=0?C.accent:C.red,opacity:0.5,borderRadius:2}}/></div>
                  <span style={{fontSize:9,fontWeight:700,color:C.mutedLight,width:40,textAlign:"right"}}>{m.idxYtd>=0?"+":""}{m.idxYtd}%</span>
                </div>
                <div style={{...row,fontSize:10}}><span style={{color:C.muted}}>{cnt} stocks</span><span style={{fontWeight:700}}>{fmtS(portVal)}</span></div>
              </div>
            </div>
          );
        })}
        <div style={card}>
          <div style={cardT}>Sector Breakdown by Market (Charles Schwab Classification)</div>
          {[...new Set(holdings.map(h=>h.mkt))].map(mkt=>{
            const m=MKT[mkt]||MKT.US;
            const mktHoldings=holdings.filter(h=>h.mkt===mkt);
            const mktTotal=mktHoldings.reduce((s,h)=>s+toSGD(h.price*h.shares,h.mkt),0);
            if(mktTotal===0)return null;
            const sectorsInMkt=SECTORS.map((sec,i)=>{
              const secHoldings=mktHoldings.filter(h=>h.sector===sec);
              const val=secHoldings.reduce((s,h)=>s+toSGD(h.price*h.shares,h.mkt),0);
              return{sec,val,col:SCOL[i],cnt:secHoldings.length};
            }).filter(d=>d.val>0).sort((a,b)=>b.val-a.val);
            return(
              <div key={mkt} style={{marginBottom:16}}>
                <div style={{display:"flex",alignItems:"center",gap:6,marginBottom:8}}>
                  <Chip mkt={mkt}/>
                  <span style={{fontWeight:700,fontSize:13}}>{m.index}</span>
                  <span style={{fontSize:10,color:C.muted}}>{mktHoldings.length} stocks</span>
                </div>
                {sectorsInMkt.map(({sec,val,col,cnt})=>(
                  <div key={sec} style={{marginBottom:6}}>
                    <div style={{display:"flex",justifyContent:"space-between",fontSize:11,marginBottom:3}}>
                      <span style={{display:"flex",alignItems:"center",gap:5}}>
                        <div style={{width:6,height:6,borderRadius:3,background:col}}/>
                        <span style={{color:C.text}}>{sec}</span>
                        <span style={{fontSize:9,color:C.muted}}>{cnt} stock{cnt>1?"s":""}</span>
                      </span>
                      <span style={{fontWeight:700,color:col}}>{mktTotal>0?((val/mktTotal)*100).toFixed(1):0}%</span>
                    </div>
                    <div style={{height:4,borderRadius:2,background:C.border}}>
                      <div style={{width:`${mktTotal>0?(val/mktTotal)*100:0}%`,height:"100%",borderRadius:2,background:col}}/>
                    </div>
                  </div>
                ))}
              </div>
            );
          })}
        </div>
      </>
    );
  }

  // ── Trades tab ───────────────────────────────────────────────────────────────
  function TradesView(){
    const shown=tradeType==="ALL"?trades:trades.filter(t=>t.type===tradeType);
    const totalReal=trades.filter(t=>t.type==="SELL").reduce((s,t)=>s+toSGD(t.profit||0,t.mkt),0);
    const mkts=Object.keys(MKT);
    const ccyList=Object.keys(CCY);
    const iField={width:"100%",background:C.card,border:`1px solid ${C.border}`,borderRadius:7,padding:"7px 10px",color:C.text,fontSize:12,outline:"none",boxSizing:"border-box"};
    const lbl={fontSize:10,color:C.muted,marginBottom:3};
    const tradePriceSym=ccySymbol(tradeForm.ccy);
    const tradePriceTotal=parseFloat(tradeForm.price||0)*parseInt(tradeForm.shares||0);
    return(
      <>
        <div style={{...card,background:C.accent+"08",border:`1px solid ${C.accentDim}25`,marginBottom:12}}>
          <div style={{display:"grid",gridTemplateColumns:"1fr 1fr 1fr",gap:8,textAlign:"center"}}>
            <div><div style={{fontSize:9,color:C.muted}}>Total</div><div style={{fontSize:20,fontWeight:800}}>{trades.length}</div></div>
            <div><div style={{fontSize:9,color:C.muted}}>Sells</div><div style={{fontSize:20,fontWeight:800,color:C.gold}}>{trades.filter(t=>t.type==="SELL").length}</div></div>
            <div><div style={{fontSize:9,color:C.muted}}>Realized P&amp;L</div><div style={{fontSize:14,fontWeight:800,color:totalReal>=0?C.green:C.red}}>{totalReal>=0?"+":"-"}{fmtS(Math.abs(totalReal))}</div></div>
          </div>
        </div>

        {/* Add / Edit Trade Button */}
        <button onClick={()=>{if(showTradeForm&&editTradeId==null){setShowTradeForm(false);}else{setShowTradeForm(v=>!v);setEditTradeId(null);setTradeForm({ticker:"",type:"BUY",date:new Date().toISOString().slice(0,10),price:"",shares:"",mkt:"US",ccy:"USD"});setTickerCheck({status:"idle",message:"",suggestions:[]});setTickerSearchTerm("");}}} style={{width:"100%",padding:"11px",borderRadius:10,border:`1px dashed ${showTradeForm?C.accent:C.border}`,background:showTradeForm&&editTradeId==null?C.accent+"12":"transparent",color:showTradeForm&&editTradeId==null?C.accent:C.muted,fontSize:13,fontWeight:700,cursor:"pointer",marginBottom:10}}>
          {showTradeForm&&editTradeId==null?"✕ Cancel":"+ Add New Trade"}
        </button>

        {/* Trade Entry / Edit Form */}
        {showTradeForm&&(
          <div style={{...card,border:`1px solid ${editTradeId!=null?C.gold:C.accent}40`,background:C.surface,marginBottom:14}}>
            <div style={{display:"flex",justifyContent:"space-between",alignItems:"center",marginBottom:12}}>
              <div style={{fontSize:12,fontWeight:700,color:editTradeId!=null?C.gold:C.accent}}>
                {editTradeId!=null?"EDIT TRADE":"NEW TRADE ENTRY"}
              </div>
              {editTradeId!=null&&<button onClick={()=>{setShowTradeForm(false);setEditTradeId(null);}} style={{background:"none",border:"none",color:C.muted,fontSize:16,cursor:"pointer"}}>✕</button>}
            </div>

            {/* Row 1: Ticker search + Type */}
            <div style={{marginBottom:8}}>
              <div style={lbl}>Stock Search — Name or Ticker Symbol</div>
              <div style={{display:"flex",gap:6}}>
                <input style={{...iField,flex:1}} placeholder="e.g. NVIDIA or NVDA or D05.SI" value={tickerSearchTerm} onChange={e=>{setTickerSearchTerm(e.target.value);setTickerCheck({status:"idle",message:"",suggestions:[]});}} onKeyDown={e=>{if(e.key==="Enter"){e.preventDefault();lookupTicker(tickerSearchTerm);}}}/>
                <button onClick={()=>lookupTicker(tickerSearchTerm)} style={{padding:"7px 12px",borderRadius:7,border:`1px solid ${C.accent}`,background:C.accent+"18",color:C.accent,fontSize:11,fontWeight:700,cursor:"pointer",flexShrink:0,whiteSpace:"nowrap"}}>
                  {tickerCheck.status==="loading"?"...":"Search"}
                </button>
              </div>
              {/* Validation feedback */}
              {tickerCheck.status==="found"&&(
                <div style={{marginTop:6,padding:"6px 10px",background:C.green+"15",border:`1px solid ${C.green}44`,borderRadius:6}}>
                  <div style={{display:"flex",justifyContent:"space-between",alignItems:"center"}}>
                    <div>
                      <div style={{fontSize:10,color:C.green,fontWeight:700}}>Confirmed: {tickerCheck.confirmed}</div>
                      <div style={{fontSize:11,color:C.text}}>{tickerCheck.message}</div>
                    </div>
                    <div style={{fontSize:10,fontWeight:700,padding:"2px 8px",borderRadius:5,background:C.green,color:"#000"}}>OK</div>
                  </div>
                  {tickerCheck.suggestions&&tickerCheck.suggestions.length>0&&(
                    <div style={{marginTop:6,fontSize:10,color:C.muted}}>
                      Also matches: {tickerCheck.suggestions.map((s,i)=>(
                        <button key={i} onClick={()=>{setTradeForm(f=>({...f,ticker:s.ticker}));setTickerCheck(prev=>({...prev,status:"found",message:s.name,confirmed:s.ticker,suggestions:[]}));setTickerSearchTerm(s.ticker);}} style={{marginLeft:4,padding:"1px 6px",borderRadius:4,border:`1px solid ${C.accent}`,background:"transparent",color:C.accent,fontSize:10,cursor:"pointer"}}>
                          {s.ticker}
                        </button>
                      ))}
                    </div>
                  )}
                </div>
              )}
              {tickerCheck.status==="suggestions"&&(
                <div style={{marginTop:6,padding:"6px 10px",background:C.gold+"12",border:`1px solid ${C.gold}44`,borderRadius:6}}>
                  <div style={{fontSize:10,color:C.gold,fontWeight:700,marginBottom:5}}>{tickerCheck.message}</div>
                  <div style={{display:"flex",gap:5,flexWrap:"wrap"}}>
                    {tickerCheck.suggestions.map((s,i)=>(
                      <button key={i} onClick={()=>{setTradeForm(f=>({...f,ticker:s.ticker}));setTickerCheck({status:"found",message:s.name,confirmed:s.ticker,suggestions:[]});setTickerSearchTerm(s.ticker);}} style={{padding:"4px 8px",borderRadius:5,border:`1px solid ${C.gold}66`,background:C.gold+"12",color:C.text,fontSize:11,cursor:"pointer",textAlign:"left"}}>
                        <div style={{fontWeight:700,fontSize:11}}>{s.ticker}</div>
                        <div style={{fontSize:9,color:C.muted}}>{s.name}</div>
                      </button>
                    ))}
                  </div>
                </div>
              )}
              {tickerCheck.status==="error"&&(
                <div style={{marginTop:5,fontSize:10,color:C.red}}>{tickerCheck.message}</div>
              )}
            </div>

            {/* Confirmed ticker + trade type row */}
            <div style={{display:"grid",gridTemplateColumns:"1fr 1fr",gap:8,marginBottom:8}}>
              <div>
                <div style={lbl}>Ticker Symbol {tickerCheck.status==="found"?"(confirmed)":"(manual entry)"}</div>
                <input style={{...iField,borderColor:tickerCheck.status==="found"?C.green:tickerCheck.status==="suggestions"?C.gold:C.border}} placeholder="TICKER" value={tradeForm.ticker} onChange={e=>setTradeForm(f=>({...f,ticker:e.target.value.toUpperCase()}))}/>
              </div>
              <div>
                <div style={lbl}>Trade Type</div>
                <div style={{display:"flex",gap:4}}>
                  {["BUY","SELL"].map(t=>(
                    <button key={t} onClick={()=>setTradeForm(f=>({...f,type:t}))} style={{flex:1,padding:"7px",borderRadius:7,border:`1px solid ${tradeForm.type===t?(t==="BUY"?C.green:C.red):C.border}`,background:tradeForm.type===t?(t==="BUY"?C.green+"22":C.red+"22"):"transparent",color:tradeForm.type===t?(t==="BUY"?C.green:C.red):C.muted,fontSize:12,fontWeight:700,cursor:"pointer"}}>{t}</button>
                  ))}
                </div>
              </div>
            </div>

            {/* Row 2: Date */}
            <div style={{marginBottom:8}}>
              <div style={lbl}>Trade Date</div>
              <input type="date" style={iField} value={tradeForm.date} onChange={e=>setTradeForm(f=>({...f,date:e.target.value}))}/>
            </div>

            {/* Row 3: Exchange/Country separate from Currency */}
            <div style={{display:"grid",gridTemplateColumns:"1fr 1fr",gap:8,marginBottom:8}}>
              <div>
                <div style={lbl}>Exchange / Country</div>
                <select style={iField} value={tradeForm.mkt} onChange={e=>setTradeForm(f=>({...f,mkt:e.target.value}))}>
                  {mkts.map(mk=><option key={mk} value={mk}>{mk} — {MKT[mk].index}</option>)}
                </select>
                <div style={{fontSize:9,color:C.muted,marginTop:3}}>Where the stock is listed</div>
              </div>
              <div>
                <div style={lbl}>Currency of Trade</div>
                <select style={iField} value={tradeForm.ccy} onChange={e=>setTradeForm(f=>({...f,ccy:e.target.value}))}>
                  {ccyList.map(c=><option key={c} value={c}>{c} ({CCY[c].symbol})</option>)}
                </select>
                <div style={{fontSize:9,color:C.muted,marginTop:3}}>Price currency (e.g. SG stock in USD)</div>
              </div>
            </div>

            {/* Row 4: Price + Units */}
            <div style={{display:"grid",gridTemplateColumns:"1fr 1fr",gap:8,marginBottom:8}}>
              <div>
                <div style={lbl}>Price ({tradeForm.ccy} {tradePriceSym})</div>
                <input type="number" style={iField} placeholder="0.00" value={tradeForm.price} onChange={e=>setTradeForm(f=>({...f,price:e.target.value}))}/>
              </div>
              <div>
                <div style={lbl}>Units / Shares</div>
                <input type="number" style={iField} placeholder="0" value={tradeForm.shares} onChange={e=>setTradeForm(f=>({...f,shares:e.target.value}))}/>
              </div>
            </div>

            {/* Preview */}
            {tradeForm.price&&tradeForm.shares&&tradePriceTotal>0&&(
              <div style={{background:C.card,borderRadius:7,padding:"8px 10px",marginBottom:8,fontSize:11,display:"flex",justifyContent:"space-between",alignItems:"center"}}>
                <span style={{color:C.muted}}>Total:</span>
                <span style={{fontWeight:700}}>
                  {tradePriceSym}{fmt(tradePriceTotal,0)} {tradeForm.ccy}
                  <span style={{color:C.muted,fontWeight:400}}> ≈ {fmtS(ccyToSGD(tradePriceTotal,tradeForm.ccy))}</span>
                </span>
              </div>
            )}
            <button onClick={submitTrade} style={{width:"100%",padding:"11px",borderRadius:8,border:"none",background:tradeForm.type==="BUY"?C.green:C.red,color:"#000",fontSize:13,fontWeight:700,cursor:"pointer"}}>
              {editTradeId!=null?"Save Changes":"Confirm"} {tradeForm.type} — {tradeForm.ticker||"ticker"}
            </button>
          </div>
        )}

        <div style={{display:"flex",gap:6,marginBottom:10}}>
          {["ALL","BUY","SELL"].map(t=><button key={t} style={pill(tradeType===t)} onClick={()=>setTradeType(t)}>{t}</button>)}
        </div>

        {shown.slice(0,100).map((t,i)=>{
          const sym=ccySymbol(t.ccy||t.mkt);
          const localTotal=t.shares*t.price;
          const sgdTotal=ccyToSGD(localTotal,t.ccy||t.mkt);
          const isEditing=editTradeId===t.id;
          const stockName=tickerNames[t.ticker]||"";
          return(
            <div key={t.id||i} style={{...card,borderLeft:`3px solid ${t.type==="BUY"?C.green:C.red}`,background:isEditing?C.gold+"08":C.card}}>
              <div style={row}>
                <div style={{flex:1,minWidth:0}}>
                  <div style={{display:"flex",gap:6,alignItems:"center",marginBottom:2}}>
                    <span style={{fontWeight:800,fontSize:14}}>{t.ticker}</span>
                    <Tag col={t.type==="BUY"?C.green:C.red}>{t.type}</Tag>
                    <Chip mkt={t.mkt}/>
                    {t.ccy&&t.ccy!==(MKT[t.mkt]?.code)&&(
                      <span style={{fontSize:9,fontWeight:700,padding:"1px 5px",borderRadius:3,background:C.gold+"22",color:C.gold}}>{t.ccy}</span>
                    )}
                  </div>
                  {stockName&&<div style={{fontSize:10,color:C.mutedLight,marginBottom:2,whiteSpace:"nowrap",overflow:"hidden",textOverflow:"ellipsis",maxWidth:200}}>{stockName}</div>}
                  <div style={{fontSize:11,color:C.muted}}>{t.date} · {t.shares?.toLocaleString()} @ {sym}{fmt(t.price)}</div>
                  {t.type==="SELL"&&t.profit!=null&&t.profit!==0&&<div style={{fontSize:11,fontWeight:700,color:t.profit>=0?C.green:C.red,marginTop:2}}>P&amp;L: {t.profit>=0?"+":"-"}{sym}{fmt(Math.abs(t.profit),0)} <span style={{color:C.muted,fontWeight:400}}>({t.profit>=0?"+":"-"}{fmtS(Math.abs(ccyToSGD(t.profit,t.ccy||t.mkt)))})</span></div>}
                </div>
                <div style={{display:"flex",flexDirection:"column",alignItems:"flex-end",gap:4,flexShrink:0,marginLeft:8}}>
                  <div style={{textAlign:"right"}}>
                    <div style={{fontSize:13,fontWeight:800,color:t.type==="BUY"?C.red:C.green}}>{t.type==="BUY"?"-":"+"}{sym}{fmt(localTotal,0)}</div>
                    <div style={{fontSize:9,color:C.muted}}>{t.type==="BUY"?"-":"+"}{fmtS(sgdTotal)}</div>
                  </div>
                  <div style={{display:"flex",gap:5}}>
                    <button onClick={()=>startEditTrade(t)} style={{fontSize:10,padding:"3px 8px",borderRadius:5,border:`1px solid ${C.border}`,background:"transparent",color:C.accent,cursor:"pointer",fontWeight:600}}>Edit</button>
                    <button onClick={()=>deleteTrade(t.id)} style={{fontSize:10,padding:"3px 8px",borderRadius:5,border:`1px solid ${C.red}44`,background:"transparent",color:C.red,cursor:"pointer",fontWeight:600}}>Del</button>
                  </div>
                </div>
              </div>
            </div>
          );
        })}
        {shown.length>100&&<div style={{textAlign:"center",color:C.muted,fontSize:12,padding:"10px 0"}}>Showing 100 of {shown.length}</div>}
      </>
    );
  }

  // ── Summary tab ──────────────────────────────────────────────────────────────
  function SummaryView(){
    return(
      <>
        <div style={card}>
          <div style={cardT}>Portfolio Overview (SGD)</div>
          <div style={{display:"grid",gridTemplateColumns:"1fr 1fr",gap:8,marginBottom:8}}>
            <div style={sbox(C.accent)}><div style={{fontSize:9,color:C.muted}}>Total Value</div><div style={{fontSize:17,fontWeight:800}}>{fmtS(totalValSGD)}</div><div style={{fontSize:9,color:C.muted}}>{holdings.length} stocks</div></div>
            <div style={sbox()}><div style={{fontSize:9,color:C.muted}}>Total Cost</div><div style={{fontSize:17,fontWeight:800}}>{fmtS(totalCostSGD)}</div><div style={{fontSize:9,color:C.muted}}>{totalShares.toLocaleString()} shares</div></div>
            <div style={sbox(unrealSGD>=0?C.green:C.red)}><div style={{fontSize:9,color:C.muted}}>Unrealized P&amp;L</div><div style={{fontSize:15,fontWeight:800,color:unrealSGD>=0?C.green:C.red}}>{unrealSGD>=0?"+":"-"}{fmtS(Math.abs(unrealSGD))}</div><div style={{fontSize:11,fontWeight:700,color:unrealSGD>=0?C.green:C.red}}>{fmtPct(unrealPct)}</div></div>
            <div style={sbox(realizedSGD>=0?C.gold:C.red)}><div style={{fontSize:9,color:C.muted}}>Realized P&amp;L</div><div style={{fontSize:15,fontWeight:800,color:realizedSGD>=0?C.gold:C.red}}>{realizedSGD>=0?"+":"-"}{fmtS(Math.abs(realizedSGD))}</div><div style={{fontSize:9,color:C.muted}}>Closed trades</div></div>
            <div style={{...sbox(C.purple),textAlign:"center"}}><div style={{fontSize:9,color:C.muted}}>Stocks</div><div style={{fontSize:22,fontWeight:800,color:C.purple}}>{holdings.length}</div><div style={{fontSize:9,color:C.muted}}>{totalShares.toLocaleString()} sh</div></div>
            <div style={{...sbox(C.gold),textAlign:"center"}}><div style={{fontSize:9,color:C.muted}}>Annual Div</div><div style={{fontSize:14,fontWeight:800,color:C.gold}}>{fmtS(totalDivSGD)}</div><div style={{fontSize:9,color:C.muted}}>{fmt(totalValSGD?totalDivSGD/totalValSGD*100:0)}% yield</div></div>
          </div>
        </div>
        <div style={card}>
          <div style={cardT}>Market Exposure — All Countries</div>
          {[...new Set(holdings.map(h=>h.mkt))].map((mktKey,i)=>{
            const m=MKT[mktKey]||MKT.US;
            const col=[C.accent,C.green,C.gold,C.purple,C.red,"#FF8C42","#62D2E8"][i%7];
            const mktHoldings=holdings.filter(h=>h.mkt===mktKey);
            const localVal=mktHoldings.reduce((s,h)=>s+h.price*h.shares,0);
            const sgdVal=mktHoldings.reduce((s,h)=>s+toSGD(h.price*h.shares,h.mkt),0);
            const sgdCost=mktHoldings.reduce((s,h)=>s+toSGD(h.avgCost*h.shares,h.mkt),0);
            const pnl=sgdVal-sgdCost;
            const pnlPct=sgdCost?(pnl/sgdCost)*100:0;
            const pct=totalValSGD?(sgdVal/totalValSGD)*100:0;
            return(
              <div key={mktKey} style={{marginBottom:12,paddingBottom:12,borderBottom:i<[...new Set(holdings.map(h=>h.mkt))].length-1?`1px solid ${C.border}`:"none"}}>
                <div style={{display:"flex",justifyContent:"space-between",alignItems:"flex-start",marginBottom:4}}>
                  <div style={{display:"flex",alignItems:"center",gap:6}}>
                    <div style={{width:10,height:10,borderRadius:5,background:col,flexShrink:0}}/>
                    <div>
                      <div style={{display:"flex",alignItems:"center",gap:5}}>
                        <span style={{fontWeight:700,fontSize:13}}>{mktKey}</span>
                        <Chip mkt={mktKey}/>
                        <span style={{fontSize:10,color:C.muted}}>{m.index}</span>
                      </div>
                      <div style={{fontSize:10,color:C.muted}}>{mktHoldings.length} stocks · {m.symbol}{fmt(localVal,0)} {m.code}</div>
                    </div>
                  </div>
                  <div style={{textAlign:"right"}}>
                    <div style={{fontWeight:700,fontSize:13}}>{fmtS(sgdVal)}</div>
                    <div style={{fontSize:10,fontWeight:700,color:pnl>=0?C.green:C.red}}>{pnl>=0?"+":"-"}{fmtS(Math.abs(pnl))} ({pnl>=0?"+":""}{fmt(pnlPct,1)}%)</div>
                    <div style={{fontSize:9,color:C.muted}}>{pct.toFixed(1)}% of portfolio</div>
                  </div>
                </div>
                <div style={{height:5,borderRadius:3,background:C.border}}>
                  <div style={{width:`${pct}%`,height:"100%",borderRadius:3,background:col}}/>
                </div>
              </div>
            );
          })}
        </div>
      </>
    );
  }

  // ── Holding detail modal ─────────────────────────────────────────────────────
  function HoldingDetail(){
    const h=sel;if(!h)return null;
    const m=MKT[h.mkt]||MKT.US,sc=scoreH(h),r=getRec(h),bs=buffettScore(h);
    const gainPct=((h.price-h.avgCost)/h.avgCost)*100,upside=((h.intrinsic-h.price)/h.price)*100;
    const localVal=h.price*h.shares,localCost=h.avgCost*h.shares,localGain=localVal-localCost,localDiv=(h.divYield/100)*localVal;
    const sgdVal=toSGD(localVal,h.mkt),sgdCost=toSGD(localCost,h.mkt),sgdGain=toSGD(localGain,h.mkt),sgdDiv=toSGD(localDiv,h.mkt);
    const w=wtTotal(h),pos=gainPct>=0;
    const analysis=aiText[h.ticker],loading=aiLoad[h.ticker];
    const buyHist=trades.filter(t=>t.ticker===h.ticker&&t.type==="BUY").sort((a,b)=>a.date.localeCompare(b.date));
    const sellHist=trades.filter(t=>t.ticker===h.ticker&&t.type==="SELL").sort((a,b)=>b.date.localeCompare(a.date));
    return(
      <div style={modal} onClick={e=>{if(e.target===e.currentTarget)setSel(null);}}>
        <div style={mCard}>
          <div style={{display:"flex",justifyContent:"space-between",alignItems:"flex-start",marginBottom:12}}>
            <div>
              <div style={{fontWeight:800,fontSize:17,display:"flex",alignItems:"center",gap:7}}>{h.ticker}<Chip mkt={h.mkt}/></div>
              <div style={{fontSize:12,color:C.muted}}>{h.name}</div>
              <div style={{fontSize:10,color:C.mutedLight,marginTop:2}}>{m.index} · YTD {m.idxYtd>=0?"+":""}{m.idxYtd}%</div>
            </div>
            <div style={{display:"flex",gap:6,alignItems:"center"}}>
              <button onClick={()=>openEditHolding(h)} style={{fontSize:11,padding:"5px 10px",borderRadius:6,border:`1px solid ${C.accent}`,background:C.accent+"12",color:C.accent,cursor:"pointer",fontWeight:700}}>Edit</button>
              <button onClick={()=>{setSel(null);confirmDeleteHolding(h.id);}} style={{fontSize:11,padding:"5px 10px",borderRadius:6,border:`1px solid ${C.red}55`,background:C.red+"12",color:C.red,cursor:"pointer",fontWeight:700}}>Delete</button>
              <button onClick={()=>setSel(null)} style={{background:"none",border:"none",color:C.muted,fontSize:22,cursor:"pointer",lineHeight:1,marginLeft:2}}>x</button>
            </div>
          </div>

          {/* Avg Cost / Price / Intrinsic */}
          <div style={{display:"grid",gridTemplateColumns:"1fr 1fr 1fr",gap:8,marginBottom:10}}>
            <div style={{background:C.surface,borderRadius:9,padding:"10px 10px"}}>
              <div style={{fontSize:9,color:C.muted,marginBottom:2}}>Avg Cost</div>
              <div style={{fontSize:15,fontWeight:800}}>{fmtL(h.avgCost,h.mkt)}</div>
              <div style={{fontSize:9,color:C.muted}}>{fmtS(toSGD(h.avgCost,h.mkt))}</div>
            </div>
            <div style={{background:C.surface,borderRadius:9,padding:"10px 10px"}}>
              <div style={{fontSize:9,color:C.muted,marginBottom:2}}>Price ({m.code})</div>
              <div style={{fontSize:15,fontWeight:800}}>{fmtL(h.price,h.mkt)}</div>
              <div style={{fontSize:9,color:pos?C.green:C.red,fontWeight:700}}>{fmtPct(gainPct)}</div>
            </div>
            <div style={{background:C.surface,borderRadius:9,padding:"10px 10px"}}>
              <div style={{fontSize:9,color:C.muted,marginBottom:2}}>Intrinsic</div>
              <div style={{fontSize:15,fontWeight:800}}>{fmtL(h.intrinsic,h.mkt)}</div>
              <div style={{fontSize:9,color:upside>=0?C.green:C.red,fontWeight:700}}>{upside>=0?"+":""}{fmt(upside,1)}%</div>
            </div>
          </div>

          {/* Multi-period chart */}
          <div style={{...card,padding:12,marginBottom:10}}>
            <div style={{...row,marginBottom:8}}>
              <div style={{fontSize:10,color:C.muted,fontWeight:700}}>History ({m.code})</div>
              <div style={{display:"flex",gap:3}}>
                {PERIODS.map(p=><button key={p} style={smPill(detailPeriod===p)} onClick={()=>setDetailPeriod(p)}>{PLBL[p]}</button>)}
              </div>
            </div>
            <Sparkline data={HIST[h.ticker]?.[detailPeriod]||[]} color={pos?C.green:C.red} height={60}/>
            <div style={{display:"flex",justifyContent:"space-between",fontSize:9,color:C.muted,marginTop:3}}>
              <span>{detailPeriod==="all"&&FIRST_BUY[h.ticker]?"First buy: "+FIRST_BUY[h.ticker]:{"30d":"30 days ago","6m":"6 months ago","1y":"1 year ago","5y":"5 years ago","all":"Start"}[detailPeriod]}</span>
              <span>{fmtL(h.price,h.mkt)}</span>
            </div>
          </div>

          {/* Position */}
          <div style={{background:C.accent+"0D",border:`1px solid ${C.accentDim}30`,borderRadius:10,padding:"12px 14px",marginBottom:10}}>
            <div style={{fontSize:9,color:C.accent,fontWeight:700,letterSpacing:"0.08em",marginBottom:8}}>POSITION</div>
            <div style={{display:"grid",gridTemplateColumns:"1fr 1fr",gap:"8px 14px",marginBottom:8}}>
              {[["Shares",h.shares.toLocaleString(),null],["Avg Cost",fmtL(h.avgCost,h.mkt),fmtS(toSGD(h.avgCost,h.mkt))],["Market Value",fmtL(localVal,h.mkt,0),fmtS(sgdVal)],["Cost Basis",fmtL(localCost,h.mkt,0),fmtS(sgdCost)],["Unrealized P&L",`${pos?"+":"-"}${fmtL(Math.abs(localGain),h.mkt,0)}`,`${pos?"+":"-"}${fmtS(Math.abs(sgdGain))}`],["Annual Div (est.)",fmtL(localDiv,h.mkt,0),fmtS(sgdDiv)]].map(([l,v,sub])=>(
                <div key={l}><div style={{fontSize:9,color:C.muted}}>{l}</div><div style={{fontSize:13,fontWeight:700,color:l==="Unrealized P&L"?(pos?C.green:C.red):C.text}}>{v}</div>{sub&&<div style={{fontSize:9,color:C.muted}}>{sub}</div>}</div>
              ))}
            </div>
            <div style={{...row,marginBottom:3}}><span style={{fontSize:9,color:C.muted}}>Portfolio Weight</span><span style={{fontSize:12,fontWeight:800,color:C.accent}}>{w.toFixed(2)}%</span></div>
            <div style={{height:4,borderRadius:2,background:C.border}}><div style={{width:`${Math.min(w*3,100)}%`,height:"100%",borderRadius:2,background:C.accent}}/></div>
          </div>

          {/* Buffett score */}
          <div style={{background:"#1A1200",border:`1px solid ${C.gold}30`,borderRadius:10,padding:"10px 14px",marginBottom:10}}>
            <div style={{...row}}>
              <div><div style={{fontSize:9,color:C.gold,fontWeight:700,marginBottom:3}}>BUFFETT SCORE</div><div style={{fontSize:12,color:C.mutedLight}}>{bs.reason}</div></div>
              <div style={{textAlign:"right"}}>
                <div style={{fontSize:24,fontWeight:800,color:bs.score>=65?C.green:bs.score>=35?C.gold:C.red}}>{bs.score}<span style={{fontSize:12,color:C.muted}}>/100</span></div>
                <Bdg label={bs.action} bg={bs.col+"22"} color={bs.col}/>
              </div>
            </div>
          </div>

          {buyHist.length>0&&(
            <div style={card}>
              <div style={cardT}>Buy History ({buyHist.length} lots)</div>
              {buyHist.slice(0,10).map((bt,i)=>(
                <div key={i} style={{display:"flex",justifyContent:"space-between",fontSize:12,marginBottom:5,paddingBottom:5,borderBottom:i<Math.min(buyHist.length,10)-1?`1px solid ${C.border}`:"none"}}>
                  <span style={{color:C.muted}}>{bt.date}</span>
                  <div style={{display:"flex",gap:12}}><span style={{color:C.mutedLight}}>{bt.shares.toLocaleString()} sh</span><span style={{fontWeight:700,color:C.green}}>{fmtL(bt.price,h.mkt)}</span></div>
                </div>
              ))}
              {buyHist.length>10&&<div style={{fontSize:10,color:C.muted,textAlign:"center"}}>+{buyHist.length-10} more lots</div>}
            </div>
          )}
          {sellHist.length>0&&(
            <div style={card}>
              <div style={cardT}>Sell History ({sellHist.length} trades)</div>
              {sellHist.slice(0,5).map((st,i)=>(
                <div key={i} style={{display:"flex",justifyContent:"space-between",fontSize:12,marginBottom:5,paddingBottom:5,borderBottom:i<Math.min(sellHist.length,5)-1?`1px solid ${C.border}`:"none"}}>
                  <span style={{color:C.muted}}>{st.date}</span>
                  <div style={{display:"flex",gap:10}}><span style={{color:C.mutedLight}}>{st.shares.toLocaleString()} sh</span><span style={{fontWeight:700,color:C.red}}>{fmtL(st.price,h.mkt)}</span>{st.profit!=null&&<span style={{fontWeight:700,color:st.profit>=0?C.green:C.red}}>{st.profit>=0?"+":"-"}{fmtL(Math.abs(st.profit),h.mkt,0)}</span>}</div>
                </div>
              ))}
            </div>
          )}

          <div style={card}><div style={cardT}>Analysis Scores</div>{[["Intrinsic Value",sc.iv],["Economic Moat",sc.mt],["Dividend Yield",sc.dv],["Overall",sc.all]].map(([l,v])=>(<div key={l} style={{marginBottom:8}}><div style={{fontSize:12,color:l==="Overall"?C.text:C.muted,marginBottom:3,fontWeight:l==="Overall"?700:400}}>{l}</div><ScoreBar score={v} max={10} color={l==="Overall"?C.accent:undefined}/></div>))}</div>
          <div style={card}><div style={cardT}>Key Stats</div><div style={{display:"grid",gridTemplateColumns:"1fr 1fr",gap:"8px 12px"}}>{[["P/E",fmt(h.peRatio)],["Div Yield",fmt(h.divYield)+"%"],["Sector",h.sector],["MS Style",h.msStyle],["Market",`${h.mkt} (${m.code})`],["Benchmark",m.index]].map(([l,v])=>(<div key={l}><div style={{fontSize:9,color:C.muted}}>{l}</div><div style={{fontSize:12,fontWeight:600}}>{v}</div></div>))}</div></div>

          <div style={{...card,background:C.accent+"08",border:`1px solid ${C.accentDim}30`}}>
            <div style={{display:"flex",gap:8,alignItems:"center",marginBottom:8}}><span style={{fontSize:16}}>AI</span><div style={cardT}>AI Analysis (Buffett Style)</div></div>
            {loading&&<div style={{textAlign:"center",padding:"14px 0",color:C.muted,fontSize:13}}>Analyzing {h.ticker}...<div style={{width:100,height:2,background:C.border,borderRadius:1,margin:"8px auto 0",overflow:"hidden"}}><div style={{width:"55%",height:"100%",background:C.accent,animation:"pulse 1s ease-in-out infinite",borderRadius:1}}/></div></div>}
            {!loading&&analysis&&<div style={{fontSize:13,color:C.mutedLight,lineHeight:1.75,whiteSpace:"pre-wrap"}}>{analysis}</div>}
            {!loading&&!analysis&&<div style={{textAlign:"center",padding:"8px 0"}}><button style={{background:C.accent,color:"#000",border:"none",borderRadius:8,padding:"8px 20px",fontSize:13,fontWeight:700,cursor:"pointer"}} onClick={()=>analyse(h)}>Generate Analysis</button></div>}
          </div>
          <div style={{height:24}}/>
        </div>
      </div>
    );
  }

  // ── Edit Holding Modal ────────────────────────────────────────────────────────
  function EditHoldingModal(){
    if(holdingEditId==null)return null;
    const f=holdingForm,setF=setHoldingForm;
    const h=holdings.find(x=>x.id===holdingEditId);
    if(!h)return null;
    const iF={width:"100%",background:C.card,border:`1px solid ${C.border}`,borderRadius:7,padding:"8px 10px",color:C.text,fontSize:13,outline:"none",boxSizing:"border-box"};
    const lbl={fontSize:10,color:C.muted,marginBottom:4};
    const mkts=Object.keys(MKT);
    const moatOpts=["Wide","Narrow","None"];
    const sectorOpts=["Technology","Healthcare","Financials","Consumer Disc.","Industrials","Energy","Utilities","Materials","Real Estate","Comm. Services","Consumer Staples"];
    const ready=f.ticker&&parseFloat(f.price)>0&&parseInt(f.shares)>0&&parseFloat(f.avgCost)>0;
    return(
      <div style={{position:"fixed",inset:0,background:"rgba(0,0,0,0.85)",display:"flex",alignItems:"flex-end",zIndex:60}} onClick={e=>{if(e.target===e.currentTarget){setHoldingEditId(null);setHoldingForm({});}}}>
        <div style={{background:C.card,borderRadius:"20px 20px 0 0",padding:20,width:"100%",maxWidth:430,margin:"0 auto",maxHeight:"92vh",overflowY:"auto"}}>
          {/* Header */}
          <div style={{display:"flex",justifyContent:"space-between",alignItems:"center",marginBottom:16}}>
            <div>
              <div style={{fontSize:15,fontWeight:800,color:C.accent}}>Edit Holding</div>
              <div style={{fontSize:11,color:C.muted}}>{h.ticker} — {h.name}</div>
            </div>
            <button onClick={()=>{setHoldingEditId(null);setHoldingForm({});}} style={{background:"none",border:"none",color:C.muted,fontSize:22,cursor:"pointer",lineHeight:1}}>x</button>
          </div>

          {/* Section: Identity */}
          <div style={{fontSize:10,color:C.accent,fontWeight:700,letterSpacing:"0.08em",marginBottom:8}}>IDENTITY</div>
          <div style={{display:"grid",gridTemplateColumns:"1fr 1fr",gap:8,marginBottom:10}}>
            <div>
              <div style={lbl}>Ticker Symbol</div>
              <input style={iF} value={f.ticker||""} onChange={e=>setF(p=>({...p,ticker:e.target.value.toUpperCase()}))}/>
            </div>
            <div>
              <div style={lbl}>Market / Exchange</div>
              <select style={iF} value={f.mkt||"US"} onChange={e=>setF(p=>({...p,mkt:e.target.value}))}>
                {mkts.map(mk=><option key={mk} value={mk}>{mk} — {MKT[mk].index}</option>)}
              </select>
            </div>
            <div style={{gridColumn:"1 / -1"}}>
              <div style={lbl}>Company Name</div>
              <input style={iF} value={f.name||""} onChange={e=>setF(p=>({...p,name:e.target.value}))} placeholder="Full company name"/>
            </div>
            <div>
              <div style={lbl}>Sector</div>
              <select style={iF} value={f.sector||"Technology"} onChange={e=>setF(p=>({...p,sector:e.target.value}))}>
                {sectorOpts.map(s=><option key={s} value={s}>{s}</option>)}
              </select>
            </div>
            <div>
              <div style={lbl}>Economic Moat</div>
              <select style={iF} value={f.moat||"Narrow"} onChange={e=>setF(p=>({...p,moat:e.target.value}))}>
                {moatOpts.map(o=><option key={o} value={o}>{o}</option>)}
              </select>
            </div>
          </div>

          {/* Section: Position */}
          <div style={{fontSize:10,color:C.accent,fontWeight:700,letterSpacing:"0.08em",marginBottom:8,marginTop:4}}>POSITION</div>
          <div style={{display:"grid",gridTemplateColumns:"1fr 1fr",gap:8,marginBottom:10}}>
            <div>
              <div style={lbl}>Shares / Units</div>
              <input type="number" style={iF} value={f.shares||""} onChange={e=>setF(p=>({...p,shares:e.target.value}))}/>
            </div>
            <div>
              <div style={lbl}>Avg Cost ({MKT[f.mkt||"US"]?.symbol})</div>
              <input type="number" style={iF} value={f.avgCost||""} onChange={e=>setF(p=>({...p,avgCost:e.target.value}))}/>
            </div>
          </div>

          {/* Section: Valuation */}
          <div style={{fontSize:10,color:C.accent,fontWeight:700,letterSpacing:"0.08em",marginBottom:8,marginTop:4}}>VALUATION</div>
          <div style={{display:"grid",gridTemplateColumns:"1fr 1fr",gap:8,marginBottom:10}}>
            <div>
              <div style={lbl}>Current Price ({MKT[f.mkt||"US"]?.symbol})</div>
              <input type="number" style={iF} value={f.price||""} onChange={e=>setF(p=>({...p,price:e.target.value}))}/>
            </div>
            <div>
              <div style={lbl}>Intrinsic Value ({MKT[f.mkt||"US"]?.symbol})</div>
              <input type="number" style={iF} value={f.intrinsic||""} onChange={e=>setF(p=>({...p,intrinsic:e.target.value}))} placeholder="Leave blank to auto-calc"/>
            </div>
            <div>
              <div style={lbl}>P/E Ratio</div>
              <input type="number" style={iF} value={f.peRatio||""} onChange={e=>setF(p=>({...p,peRatio:e.target.value}))}/>
            </div>
            <div>
              <div style={lbl}>Dividend Yield (%)</div>
              <input type="number" style={iF} value={f.divYield||""} onChange={e=>setF(p=>({...p,divYield:e.target.value}))}/>
            </div>
          </div>

          {/* Live preview */}
          {ready&&(()=>{
            const p=parseFloat(f.price),ac=parseFloat(f.avgCost),s=parseInt(f.shares);
            const gain=((p-ac)/ac)*100;
            const localVal=p*s,sgdVal=toSGD(localVal,f.mkt||"US");
            return(
              <div style={{background:C.accent+"0D",border:`1px solid ${C.accentDim}30`,borderRadius:8,padding:"10px 12px",marginBottom:12}}>
                <div style={{fontSize:9,color:C.accent,fontWeight:700,marginBottom:6}}>PREVIEW</div>
                <div style={{display:"grid",gridTemplateColumns:"1fr 1fr 1fr",gap:8,fontSize:11}}>
                  <div><div style={{fontSize:8,color:C.muted}}>Market Value</div><div style={{fontWeight:700}}>{fmtL(localVal,f.mkt||"US",0)}</div><div style={{fontSize:9,color:C.muted}}>{fmtS(sgdVal)}</div></div>
                  <div style={{textAlign:"center"}}><div style={{fontSize:8,color:C.muted}}>Gain vs Cost</div><div style={{fontWeight:700,color:gain>=0?C.green:C.red}}>{gain>=0?"+":""}{fmt(gain,1)}%</div></div>
                  <div style={{textAlign:"right"}}><div style={{fontSize:8,color:C.muted}}>P&amp;L</div><div style={{fontWeight:700,color:gain>=0?C.green:C.red}}>{gain>=0?"+":"-"}{fmtL(Math.abs((p-ac)*s),f.mkt||"US",0)}</div></div>
                </div>
              </div>
            );
          })()}

          <button onClick={saveHolding} disabled={!ready} style={{width:"100%",padding:"12px",borderRadius:9,border:"none",background:ready?C.accent:C.border,color:ready?"#000":C.muted,fontSize:14,fontWeight:700,cursor:ready?"pointer":"not-allowed",marginBottom:8}}>
            Save Changes to {f.ticker||"Holding"}
          </button>
          <button onClick={()=>{setHoldingEditId(null);setHoldingForm({});}} style={{width:"100%",padding:"10px",borderRadius:9,border:`1px solid ${C.border}`,background:"transparent",color:C.muted,fontSize:13,cursor:"pointer"}}>
            Cancel
          </button>
          <div style={{height:16}}/>
        </div>
      </div>
    );
  }

  // ── Delete Confirm Modal ──────────────────────────────────────────────────────
  function DeleteConfirmModal(){
    if(deleteConfirm==null)return null;
    const h=holdings.find(x=>x.id===deleteConfirm);
    if(!h)return null;
    const localVal=h.price*h.shares;
    const sgdVal=toSGD(localVal,h.mkt);
    return(
      <div style={{position:"fixed",inset:0,background:"rgba(0,0,0,0.88)",display:"flex",alignItems:"center",justifyContent:"center",zIndex:70,padding:"0 20px"}}>
        <div style={{background:C.card,borderRadius:16,padding:24,width:"100%",maxWidth:360,border:`1px solid ${C.red}44`}}>
          <div style={{fontSize:22,textAlign:"center",marginBottom:8}}>Delete Holding?</div>
          <div style={{textAlign:"center",marginBottom:16}}>
            <div style={{fontWeight:800,fontSize:18}}>{h.ticker}</div>
            <div style={{fontSize:12,color:C.muted}}>{h.name}</div>
            <div style={{fontSize:13,marginTop:8}}>
              <span style={{color:C.muted}}>Value: </span>
              <span style={{fontWeight:700}}>{fmtL(localVal,h.mkt,0)}</span>
              <span style={{color:C.muted}}> ≈ {fmtS(sgdVal)}</span>
            </div>
            <div style={{fontSize:13}}><span style={{color:C.muted}}>{h.shares.toLocaleString()} shares</span></div>
          </div>
          <div style={{background:C.red+"10",border:`1px solid ${C.red}33`,borderRadius:8,padding:"8px 12px",marginBottom:16,fontSize:11,color:C.red,textAlign:"center"}}>
            This will permanently remove this holding from your portfolio. This cannot be undone.
          </div>
          <div style={{display:"flex",gap:8}}>
            <button onClick={()=>setDeleteConfirm(null)} style={{flex:1,padding:"11px",borderRadius:8,border:`1px solid ${C.border}`,background:"transparent",color:C.text,fontSize:13,fontWeight:600,cursor:"pointer"}}>
              Cancel
            </button>
            <button onClick={doDeleteHolding} style={{flex:1,padding:"11px",borderRadius:8,border:"none",background:C.red,color:"#fff",fontSize:13,fontWeight:700,cursor:"pointer"}}>
              Delete
            </button>
          </div>
        </div>
      </div>
    );
  }

  // ── Shell ────────────────────────────────────────────────────────────────────
  const TABS=[
    {id:"portfolio",icon:"📊",label:"Portfolio"},
    {id:"insights", icon:"💡",label:"Insights"},
    {id:"indices",  icon:"🌍",label:"Markets"},
    {id:"trades",   icon:"💱",label:"Trades"},
    {id:"summary",  icon:"📋",label:"Summary"},
  ];
  const refreshTs=lastRefresh?lastRefresh.toLocaleTimeString("en-SG",{hour:"2-digit",minute:"2-digit",second:"2-digit"}):null;
  return(
    <div style={{fontFamily:"'DM Sans',system-ui,sans-serif",background:C.bg,minHeight:"100vh",color:C.text,maxWidth:430,margin:"0 auto",position:"relative"}}>
      {isLoading&&(
        <div style={{position:"fixed",inset:0,background:"#0A0D14",zIndex:999,display:"flex",flexDirection:"column",alignItems:"center",justifyContent:"center",gap:16,padding:20}}>
          <div style={{fontSize:28,fontWeight:800,color:"#00D4FF",letterSpacing:"-1px"}}>IGNITUS</div>
          <div style={{fontSize:12,color:"#6B7A99",textAlign:"center",maxWidth:320,wordBreak:"break-all"}}>{loadMsg}</div>
          <div style={{width:120,height:2,background:"#1E2A3E",borderRadius:1,overflow:"hidden"}}>
            <div style={{width:"55%",height:"100%",background:"#00D4FF",borderRadius:1,animation:"pulse 1s ease-in-out infinite"}}/>
          </div>
        </div>
      )}
      <style>{`@import url('https://fonts.googleapis.com/css2?family=DM+Sans:wght@400;500;600;700;800&display=swap');*{box-sizing:border-box;-webkit-tap-highlight-color:transparent;}::-webkit-scrollbar{display:none;}@keyframes pulse{0%,100%{opacity:0.4}50%{opacity:1}}@keyframes spin{from{transform:rotate(0deg)}to{transform:rotate(360deg)}}@keyframes fadeUp{from{opacity:0;transform:translateY(8px)}to{opacity:1;transform:translateY(0)}}@keyframes fadeDown{from{opacity:1;transform:translateY(0)}to{opacity:0;transform:translateY(8px)}}`}</style>
      <div style={{padding:"48px 18px 14px",background:`linear-gradient(180deg,${C.surface} 0%,${C.bg} 100%)`,borderBottom:`1px solid ${C.border}`}}>
        <div style={{display:"flex",justifyContent:"space-between",alignItems:"flex-start"}}>
          <div>
            <div style={{display:"flex",alignItems:"center",gap:6,marginBottom:3}}>
              <div style={{fontSize:9,color:C.muted,fontWeight:700,letterSpacing:"0.1em"}}>IGNITUS PORTFOLIO</div>
              <div title={dbStatus==="error"?"DB save failed":dbStatus==="saving"?"Saving...":dbStatus==="saved"?"Saved to DB":"DB ready"} style={{width:6,height:6,borderRadius:3,background:dbStatus==="error"?C.red:dbStatus==="saving"?C.gold:dbStatus==="saved"?C.green:C.border,transition:"background 0.4s"}}/>
            </div>
            <div style={{fontSize:30,fontWeight:800,letterSpacing:"-1px",lineHeight:1}}>{fmtS(totalValSGD)}</div>
            <div style={{fontSize:10,color:C.muted,marginTop:3}}>{holdings.length} stocks · {totalShares.toLocaleString()} shares{priceUpdated&&<span style={{color:C.green}}> · prices {priceUpdated.toLocaleTimeString("en-SG",{hour:"2-digit",minute:"2-digit"})}</span>}</div>
          </div>
          <div style={{textAlign:"right"}}>
            <div style={{display:"inline-flex",alignItems:"center",gap:4,padding:"3px 10px",borderRadius:20,fontSize:13,fontWeight:700,background:unrealSGD>=0?C.green+"18":C.red+"18",color:unrealSGD>=0?C.green:C.red}}>{unrealSGD>=0?"UP":"DN"} {fmtPct(unrealPct)}</div>
            <div style={{fontSize:11,color:unrealSGD>=0?C.green:C.red,fontWeight:600,marginTop:5}}>Unr. {unrealSGD>=0?"+":"-"}{fmtS(Math.abs(unrealSGD))}</div>
            <div style={{fontSize:11,color:realizedSGD>=0?C.gold:C.red,fontWeight:600,marginTop:3}}>Rlz. {realizedSGD>=0?"+":"-"}{fmtS(Math.abs(realizedSGD))}</div>
            <div style={{fontSize:10,color:C.gold,marginTop:3}}>Div {fmtS(totalDivSGD)}/yr</div>
          </div>
        </div>
      </div>

      {/* Tab bar + refresh button */}
      <div style={{display:"flex",borderBottom:`1px solid ${C.border}`,background:C.surface,position:"sticky",top:0,zIndex:10,overflowX:"auto",alignItems:"stretch"}}>
        {TABS.map(t=>(
          <button key={t.id} style={{flex:"0 0 auto",padding:"10px 12px",fontSize:10,fontWeight:tab===t.id?700:500,color:tab===t.id?C.accent:C.muted,borderBottom:`2px solid ${tab===t.id?C.accent:"transparent"}`,cursor:"pointer",background:"none",border:"none",textAlign:"center",whiteSpace:"nowrap"}} onClick={()=>setTab(t.id)}>
            <div style={{fontSize:16,marginBottom:1}}>{t.icon}</div>{t.label}
          </button>
        ))}
        {/* Refresh button — right-anchored */}
        <div style={{marginLeft:"auto",padding:"0 10px",display:"flex",alignItems:"center",flexShrink:0}}>
          {/* Price update status */}
          {priceStatus==='fetching'&&(
            <div style={{display:"flex",alignItems:"center",gap:4,padding:"4px 8px",fontSize:9,color:C.gold,fontWeight:700}}>
              <span style={{animation:"spin 1s linear infinite",display:"inline-block"}}>↻</span> Prices...
            </div>
          )}
          {priceStatus==='done'&&priceUpdated&&(
            <div style={{display:"flex",alignItems:"center",gap:4,padding:"4px 8px",fontSize:9,color:C.green,fontWeight:700,whiteSpace:"nowrap"}}>
              ✓ {priceUpdated.toLocaleTimeString("en-SG",{hour:"2-digit",minute:"2-digit"})}
            </div>
          )}
          {priceStatus==='error'&&(
            <div style={{fontSize:9,color:C.red,padding:"4px 8px",fontWeight:700}}>Price err</div>
          )}
          <button onClick={()=>fetchLivePrices(holdings)} title="Update live prices" style={{
            padding:"6px 8px",borderRadius:8,cursor:"pointer",flexShrink:0,
            border:`1px solid ${C.gold}44`,background:C.gold+"12",color:C.gold,
            fontSize:10,fontWeight:700,whiteSpace:"nowrap"
          }}>
            $ Live
          </button>
          <button onClick={doRefresh} title="Refresh all tabs" style={{
            position:"relative",display:"flex",alignItems:"center",gap:5,
            padding:"6px 10px",borderRadius:8,cursor:"pointer",
            border:`1px solid ${pendingChanges>0?C.accent:C.border}`,
            background:pendingChanges>0?C.accent+"15":"transparent",
            color:pendingChanges>0?C.accent:C.muted,
            transition:"all 0.2s"
          }}>
            <span style={{fontSize:14,display:"inline-block",animation:refreshAnim?"spin 0.6s linear":"none"}}>↻</span>
            <span style={{fontSize:10,fontWeight:700,whiteSpace:"nowrap"}}>
              {pendingChanges>0?`${pendingChanges} pending`:"Refresh"}
            </span>
            {pendingChanges>0&&(
              <span style={{position:"absolute",top:-4,right:-4,width:8,height:8,borderRadius:4,background:C.accent,animation:"pulse 1.5s ease-in-out infinite"}}/>
            )}
          </button>
        </div>
      </div>

      <div style={{overflowY:"auto",padding:"14px 16px 80px"}}>
        {/* Last refresh timestamp */}
        {refreshTs&&(
          <div style={{fontSize:9,color:C.muted,textAlign:"right",marginBottom:8,opacity:0.7}}>
            Last refreshed: {refreshTs}
          </div>
        )}
        {tab==="portfolio"&&<PortfolioView/>}
        {tab==="insights" &&<InsightsView/>}
        {tab==="indices"  &&<IndexView/>}
        {tab==="trades"   &&<TradesView/>}
        {tab==="summary"  &&<SummaryView/>}
      </div>

      {/* Floating refresh button — visible when there are pending changes */}
      {pendingChanges>0&&(
        <div style={{position:"fixed",bottom:20,right:20,zIndex:40,animation:"fadeUp 0.3s ease"}}>
          <button onClick={doRefresh} style={{
            display:"flex",alignItems:"center",gap:8,
            padding:"10px 16px",borderRadius:24,
            border:"none",cursor:"pointer",
            background:`linear-gradient(135deg,${C.accent},${C.accentDim})`,
            color:"#000",fontWeight:700,fontSize:13,
            boxShadow:`0 4px 20px ${C.accent}55`
          }}>
            <span style={{fontSize:16,display:"inline-block",animation:refreshAnim?"spin 0.6s linear":"none"}}>↻</span>
            Refresh All Tabs
            <span style={{background:"rgba(0,0,0,0.2)",borderRadius:10,padding:"1px 7px",fontSize:11}}>{pendingChanges}</span>
          </button>
        </div>
      )}

      {/* Refresh toast */}
      {refreshAnim&&(
        <div style={{position:"fixed",bottom:70,left:"50%",transform:"translateX(-50%)",zIndex:80,pointerEvents:"none",animation:"fadeUp 0.3s ease"}}>
          <div style={{background:C.green,color:"#000",padding:"8px 18px",borderRadius:20,fontSize:12,fontWeight:700,whiteSpace:"nowrap",boxShadow:`0 4px 16px ${C.green}44`}}>
            All tabs refreshed
          </div>
        </div>
      )}

      {sel&&<HoldingDetail/>}
      {holdingEditId!=null&&<EditHoldingModal/>}
      {deleteConfirm!=null&&<DeleteConfirmModal/>}
    </div>
  );
}


// ── Mount ─────────────────────────────────────────────────────────────────────
(function mountApp() {
  const loading = document.getElementById('loading');
  if (loading) loading.style.display = 'none';
  const RD = window.ReactDOM;
  if (RD && document.getElementById('root')) {
    const root = RD.createRoot(document.getElementById('root'));
    root.render(React.createElement(App));
  }
})();
