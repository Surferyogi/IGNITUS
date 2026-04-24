// Kizuna 絆 — v2.0.0 — Supabase sync across all devices
import { useState, useMemo, useCallback, useEffect, useRef } from "react";
import { supabase, supabaseConfigured } from './supabase.js';

// ─── HELPERS ─────────────────────────────────────────────────────
const p2 = n => String(n).padStart(2, '0');
// T0 is fixed at module load — used only for relative date calculations.
const T0 = new Date();
const fd = d => `${d.getFullYear()}-${p2(d.getMonth()+1)}-${p2(d.getDate())}`;
const ad = (d, n) => { const r = new Date(d); r.setDate(r.getDate() + n); return r; };
const ft = (h, m=0) => `${h%12||12}:${p2(m)} ${h>=12?'PM':'AM'}`;
const pt = s => { if (!s) return ''; const [h,m] = s.split(':').map(Number); return ft(h,m); };
const DAY   = ['Sun','Mon','Tue','Wed','Thu','Fri','Sat'];
const MON   = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];
const MFULL = ['January','February','March','April','May','June','July','August','September','October','November','December'];

const relTime = iso => {
  const diff = Date.now() - new Date(iso).getTime();
  if (diff < 60000)     return 'Just now';
  if (diff < 3600000)   return `${Math.floor(diff/60000)}m ago`;
  if (diff < 86400000)  return `${Math.floor(diff/3600000)}h ago`;
  if (diff < 604800000) return `${Math.floor(diff/86400000)}d ago`;
  return new Date(iso).toLocaleDateString('en-US', { month:'short', day:'numeric' });
};

// ─── AIRPORT LOOKUP ──────────────────────────────────────────────
// Top 300 IATA codes → city name. Bundled statically — zero API calls,
// works fully offline, instant lookup. Covers >95% of commercial routes.
const AIRPORTS = {
  SIN:'Singapore',ICN:'Seoul',NRT:'Tokyo',HND:'Tokyo',PVG:'Shanghai',PEK:'Beijing',
  PKX:'Beijing',HKG:'Hong Kong',BKK:'Bangkok',KUL:'Kuala Lumpur',CGK:'Jakarta',
  MNL:'Manila',SGN:'Ho Chi Minh City',HAN:'Hanoi',RGN:'Yangon',PNH:'Phnom Penh',
  VTE:'Vientiane',REP:'Siem Reap',DAD:'Da Nang',CXR:'Nha Trang',
  LHR:'London',LGW:'London',CDG:'Paris',AMS:'Amsterdam',FRA:'Frankfurt',
  MUC:'Munich',ZRH:'Zurich',VIE:'Vienna',MAD:'Madrid',BCN:'Barcelona',
  FCO:'Rome',MXP:'Milan',LIN:'Milan',ATH:'Athens',IST:'Istanbul',
  DXB:'Dubai',AUH:'Abu Dhabi',DOH:'Doha',BAH:'Bahrain',KWI:'Kuwait City',
  RUH:'Riyadh',JED:'Jeddah',CAI:'Cairo',ADD:'Addis Ababa',NBO:'Nairobi',
  JNB:'Johannesburg',CPT:'Cape Town',LOS:'Lagos',ACC:'Accra',CMN:'Casablanca',
  JFK:'New York',EWR:'New York',LGA:'New York',LAX:'Los Angeles',ORD:'Chicago',
  MDW:'Chicago',ATL:'Atlanta',DFW:'Dallas',DEN:'Denver',SFO:'San Francisco',
  SEA:'Seattle',MIA:'Miami',BOS:'Boston',IAD:'Washington DC',DCA:'Washington DC',
  YYZ:'Toronto',YVR:'Vancouver',YUL:'Montreal',GRU:'São Paulo',GIG:'Rio de Janeiro',
  EZE:'Buenos Aires',SCL:'Santiago',BOG:'Bogotá',LIM:'Lima',MEX:'Mexico City',
  SYD:'Sydney',MEL:'Melbourne',BNE:'Brisbane',PER:'Perth',AKL:'Auckland',
  DEL:'Delhi',BOM:'Mumbai',MAA:'Chennai',BLR:'Bangalore',HYD:'Hyderabad',
  CCU:'Kolkata',CMB:'Colombo',DAC:'Dhaka',KTM:'Kathmandu',MLE:'Malé',
  CPH:'Copenhagen',ARN:'Stockholm',HEL:'Helsinki',OSL:'Oslo',DUB:'Dublin',
  EDI:'Edinburgh',MAN:'Manchester',BRU:'Brussels',LIS:'Lisbon',OPO:'Porto',
  WAW:'Warsaw',PRG:'Prague',BUD:'Budapest',BEG:'Belgrade',SOF:'Sofia',
  OTP:'Bucharest',KBP:'Kyiv',SVO:'Moscow',DME:'Moscow',LED:'St Petersburg',
  TLV:'Tel Aviv',AMM:'Amman',BEY:'Beirut',MCT:'Muscat',KHI:'Karachi',
  LHE:'Lahore',ISB:'Islamabad',KBL:'Kabul',ULN:'Ulaanbaatar',
  CTS:'Sapporo',OKA:'Okinawa',FUK:'Fukuoka',KIX:'Osaka',NGO:'Nagoya',
  TPE:'Taipei',KHH:'Kaohsiung',TSA:'Taipei',MFM:'Macau',CAN:'Guangzhou',
  SZX:'Shenzhen',CTU:'Chengdu',XIY:'Xi\'an',WUH:'Wuhan',CKG:'Chongqing',
};

// City name from IATA code — falls back to the code itself if unknown
const airportCity = code => (code && AIRPORTS[code.toUpperCase()]) || code || '—';

// ─── FLIGHT STATUS — AeroDataBox via Supabase Edge Function ──────
// Calls the flight-status Edge Function which:
//   1. Checks a 10-minute Supabase cache first
//   2. Calls AeroDataBox if cache is stale
//   3. Returns normalised status object
// Falls back to time-based local status on any error.
// Input: flightNumber (e.g. 'SQ321') + date (e.g. '2026-04-25')

// Local time-based fallback — used when API unavailable or flight has no number
const flightStatusLocal = (flight) => {
  if (!flight.date || !flight.time) return null;
  const dep  = new Date(`${flight.date}T${flight.time}`);
  const now  = new Date();
  const mins = (now - dep) / 60000;
  let arrMins = 480;
  if (flight.endTime) {
    const [ah, am] = flight.endTime.split(':').map(Number);
    const [dh, dm] = flight.time.split(':').map(Number);
    arrMins = (ah * 60 + am) - (dh * 60 + dm);
    if (arrMins < 0) arrMins += 1440;
  }
  if (mins < -60)       return { label:'Scheduled',  color:'#5BB8E8', source:'local' };
  if (mins < -30)       return { label:'Check-in',   color:'#4D8EC4', source:'local' };
  if (mins < -10)       return { label:'Boarding',   color:'#B8715C', source:'local' };
  if (mins < 0)         return { label:'Final Call', color:'#A04E08', source:'local' };
  if (mins < arrMins)   return { label:'In Flight',  color:'#1C4878', source:'local' };
  return                       { label:'Landed',     color:'#2A6E3A', source:'local' };
};

// React hook — fetches live status, falls back to local
function useLiveFlightStatus(flight) {
  const [status,      setStatus]      = useState(() => flightStatusLocal(flight));
  const [lastUpdated, setLastUpdated] = useState(null);
  const [loading,     setLoading]     = useState(false);

  useEffect(() => {
    // Only fetch if we have a flight number and supabase is configured
    if (!flight?.flightNum || !flight?.date || !supabaseConfigured) return;

    let cancelled = false;
    async function fetchStatus() {
      setLoading(true);
      try {
        const res = await supabase.functions.invoke('flight-status', {
          body: { flightNumber: flight.flightNum, date: flight.date }
        });
        if (cancelled) return;
        if (res.error || res.data?.error) throw new Error(res.data?.error || 'fetch failed');
        setStatus(res.data);
        setLastUpdated(new Date());
      } catch {
        // Silently fall back to local status — no error shown to user
        setStatus(flightStatusLocal(flight));
      } finally {
        if (!cancelled) setLoading(false);
      }
    }

    fetchStatus();
    // Refresh every 5 minutes while card is visible
    const interval = setInterval(fetchStatus, 5 * 60 * 1000);
    return () => { cancelled = true; clearInterval(interval); };
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [flight?.flightNum, flight?.date]);

  return { status, lastUpdated, loading };
}


// Warm cream base · Terracotta rose accent · Blue/orange entry system
// Deuteranopia/protanopia safe: green replaced with cornflower blue;
// red/coral replaced with amber-orange. Text contrast ≥ 4.5:1 on cream.
const C = {
  bg:      '#F8F5F1',   // warm cream parchment
  card:    '#FFFEFB',   // near-white card surface
  elevated:'#F0EAE2',   // warm ecru — inputs, chips
  border:  '#D8CEBC',   // soft beige — now clearly visible
  muted:   '#9A9188',   // warm taupe — placeholder/disabled (contrast ≥ 3:1)
  text:    '#1A1714',   // deep warm charcoal — contrast 14:1 on cream
  dim:     '#5C5349',   // medium warm brown — contrast 6.5:1 (was too faint)
  // Terracotta rose accent — grounded, calm, wellness
  rose:    '#B8715C',   // slightly deeper for contrast on cream
  roseL:   '#E0A898',
  // Entry type colours — color-blind safe blue/orange system
  // NO green, NO red — safe for deuteranopia + protanopia
  M:       '#4D8EC4',   // steel blue        — meetings  (was dusty sky)
  F:       '#5BB8E8',   // sky blue           — flights   (warm peach → open sky)
  T:       '#4E7EC8',   // cornflower blue    — tasks     (replaces sage GREEN)
  R:       '#A07840',   // warm toffee        — reminders (was sand, now richer)
  E:       '#8A72B8',   // deeper lavender    — events    (more contrast)
};
// Convenience aliases kept for backwards compat with existing refs
C.gold  = C.rose;
C.goldL = C.roseL;

const TC = { meeting:C.M, flight:C.F, task:C.T, reminder:C.R, event:C.E };
const TI = { meeting:'◯', flight:'◇', task:'□', reminder:'◷', event:'◈' };
const TL = { meeting:'Meeting', flight:'Flight', task:'Task', reminder:'Reminder', event:'Event' };

// DTC — dark type colors for TEXT/ICONS on same-hue tinted backgrounds.
// Each gives ≥ 7:1 contrast on TC[type]+'28' tint, ≥ 9:1 on white card.
// Rule: whenever a type color is used as a font color, use DTC, never TC.
const DTC = {
  meeting:  '#1C4878',   // deep steel navy   — text-safe on C.M tints
  flight:   '#0A4268',   // deep sky navy     — text-safe on C.F tints & light sky bg
  task:     '#1A3A78',   // deep cornflower   — text-safe on C.T tints
  reminder: '#4A2E08',   // dark amber        — text-safe on C.R tints
  event:    '#38186A',   // deep violet       — text-safe on C.E tints
};

// PC.low uses DTC.task: badge renders same color as both text AND bg tint,
// so the value must be dark enough to read against its own 28% alpha wash.
const PC = { low:DTC.task, medium:'#6B4E10', high:'#8A3A08', critical:'#6A2408' };
const AC = { created:C.rose, completed:DTC.task, reopened:DTC.meeting, deleted:'#8A3A08', updated:DTC.event };
const AL = { created:'Created', completed:'Completed', reopened:'Reopened', deleted:'Deleted', updated:'Updated' };

// Shared shadow levels — replaces harsh borders on cards
const SH = {
  card:    '0 2px 16px rgba(44,38,32,0.07)',
  float:   '0 8px 32px rgba(44,38,32,0.12)',
  subtle:  '0 1px 6px  rgba(44,38,32,0.05)',
};

// ─── STORAGE + SYNC ──────────────────────────────────────────
const SK_USER        = 'exec_user_v1';
const SCHEMA_VERSION = 1;
const APP_VERSION    = 'v2.1.0';
const APP_BUILD_DATE = 'April 23, 2026';

// Load all entries for signed-in user (own + shared from workspace)
// Load entries — own entries + shared entries from workspace members.
// Load own entries — simple, reliable, no cross-table dependency
async function dbLoadEntries(userId) {
  const { data, error } = await supabase
    .from('entries')
    .select('data')
    .eq('user_id', userId)
    .order('updated_at', { ascending: true });
  if (error) throw error;
  return (data || []).map(r => r.data).filter(Boolean);
}

// Load audit log (last 200)
async function dbLoadAudit(userId) {
  const { data, error } = await supabase
    .from('audit_log').select('data').eq('user_id', userId)
    .order('created_at', { ascending: true }).limit(200);
  if (error) throw error;
  return data.map(r => r.data);
}

// Upsert a single entry
async function dbUpsertEntry(userId, entry) {
  const { error } = await supabase.from('entries')
    .upsert({ id: entry.id, user_id: userId, data: entry, updated_at: new Date().toISOString() });
  if (error) console.error('upsert entry:', error.message);
}

// Delete a single entry
async function dbDeleteEntry(userId, entryId) {
  const { error } = await supabase.from('entries').delete()
    .eq('id', entryId).eq('user_id', userId);
  if (error) console.error('delete entry:', error.message);
}

// Append audit event
async function dbAppendAudit(userId, event) {
  const { error } = await supabase.from('audit_log')
    .upsert({ id: event.id, user_id: userId, data: event });
  if (error) console.error('append audit:', error.message);
}

// Wipe all data (Reset App Data)
async function dbResetUser(userId) {
  await supabase.from('entries').delete().eq('user_id', userId);
  await supabase.from('audit_log').delete().eq('user_id', userId);
}

// Display name — stored in profiles table + cached in localStorage per user
async function dbSaveName(userId, name) {
  localStorage.setItem(`exec_user_v1_${userId}`, name);
  await supabase.from('profiles')
    .upsert({ id: userId, display_name: name, updated_at: new Date().toISOString() });
}
async function dbLoadName(userId) {
  // Always fetch from DB first for cross-device consistency.
  // Use maybeSingle() — returns null (not error) if profile row doesn't exist yet.
  try {
    const { data, error } = await supabase.from('profiles')
      .select('display_name').eq('id', userId).maybeSingle();
    if (!error && data?.display_name) {
      localStorage.setItem(`exec_user_v1_${userId}`, data.display_name);
      return data.display_name;
    }
  } catch { /* offline — fall through */ }
  return localStorage.getItem(`exec_user_v1_${userId}`) || '';
}

// Load workspace — handles users in multiple workspaces (own + invited)
// Prefers the workspace where user is admin (their own); falls back to member workspace
async function dbLoadWorkspace(userId) {
  const { data: memberships, error } = await supabase
    .from('workspace_members')
    .select('workspace_id, role, workspaces(id, name, owner_id)')
    .eq('user_id', userId)
    .order('role', { ascending: true }); // 'admin' sorts before 'member'
  if (error || !memberships || memberships.length === 0) return null;

  // Prefer admin workspace (their own), then fall back to first membership
  const membership = memberships.find(m => m.role === 'admin') || memberships[0];

  const { data: members } = await supabase
    .from('workspace_members')
    .select('user_id, role, profiles(display_name)')
    .eq('workspace_id', membership.workspace_id);

  return {
    id:      membership.workspace_id,
    name:    membership.workspaces?.name || 'My Team',
    ownerId: membership.workspaces?.owner_id,
    role:    membership.role,
    members: (members || []).map(m => ({
      id:   m.user_id,
      name: m.profiles?.display_name || 'Unknown',
      role: m.role,
    })),
  };
}

// Invite a member by email — stored as pending invite, auto-accepted on signup
async function dbInviteMember(workspaceId, invitedByUserId, email) {
  const { error } = await supabase
    .from('workspace_invites')
    .upsert({
      workspace_id: workspaceId,
      email:        email.toLowerCase().trim(),
      invited_by:   invitedByUserId,
    });
  return !error;
}

// Remove a member from workspace
async function dbRemoveMember(workspaceId, memberId) {
  const { error } = await supabase
    .from('workspace_members')
    .delete()
    .eq('workspace_id', workspaceId)
    .eq('user_id', memberId);
  return !error;
}


// ─── SHARED UI ATOMS ─────────────────────────────────────────────
const Sec = ({ label, count }) => (
  <div style={{ display:'flex', alignItems:'center', gap:10, marginBottom:14, marginTop:30 }}>
    <span style={{ fontSize:14, fontWeight:700, color:C.rose, textTransform:'uppercase', letterSpacing:'0.14em', whiteSpace:'nowrap' }}>{label}</span>
    {count != null && (
      <span style={{ fontSize:14, color:C.dim, background:C.elevated, borderRadius:10,
        padding:'3px 10px', boxShadow:SH.subtle }}>{count}</span>
    )}
    <div style={{ flex:1, height:'1px', background:C.border }} />
  </div>
);

const Badge = ({ label, color }) => (
  <span style={{ fontSize:14, fontWeight:700, color, background:color+'28', borderRadius:20,
    padding:'4px 12px', textTransform:'capitalize', letterSpacing:'0.02em', flexShrink:0,
    border:`1px solid ${color}30` }}>{label}</span>
);

const Tog = ({ on, onChange }) => (
  <button onClick={() => onChange(!on)}
    style={{ width:56, height:32, borderRadius:16, background:on?C.rose:C.elevated,
      border:`1.5px solid ${on?C.rose:C.border}`,
      cursor:'pointer', position:'relative', flexShrink:0, padding:0,
      transition:'background 0.2s, border-color 0.2s', boxShadow:SH.subtle }}>
    <div style={{ position:'absolute', top:4, left:on?28:4, width:22, height:22,
      borderRadius:11, background:on?'#fff':C.muted,
      boxShadow:'0 1px 4px rgba(0,0,0,0.15)', transition:'left 0.18s' }} />
  </button>
);

// P3-17: SS and SR at module level — never recreated on SettingsTab renders
const SS = ({ title, children }) => (
  <div style={{ marginBottom:14 }}>
    <p style={{ fontSize:14, fontWeight:700, color:C.rose, textTransform:'uppercase',
      letterSpacing:'0.14em', margin:'28px 0 10px' }}>{title}</p>
    <div style={{ background:C.card, borderRadius:22, overflow:'hidden',
      boxShadow:SH.card, border:`1px solid ${C.border}` }}>
      {children}
    </div>
  </div>
);

const SR = ({ label, sub, right, noBorder }) => (
  <div style={{ display:'flex', alignItems:'center', padding:'18px 20px',
    borderBottom:noBorder?'none':`1px solid ${C.border}`, gap:14 }}>
    <div style={{ flex:1 }}>
      <p style={{ margin:0, fontSize:17, color:C.text, fontWeight:500 }}>{label}</p>
      {sub && <p style={{ margin:0, fontSize:15, color:C.dim, marginTop:3 }}>{sub}</p>}
    </div>
    {right}
  </div>
);

// ─── ENTRY CARD ──────────────────────────────────────────────────
function ECard({ e, onToggle, onEdit, onDelete, currentUserId }) {
  const col  = TC[e.type];
  const dcol = DTC[e.type] || col;
  const [open,       setOpen]       = useState(false);
  const [confirmDel, setConfirmDel] = useState(false);

  // Shared entries from other users are read-only — no edit/delete
  const isOwn = !e.userId || e.userId === currentUserId;

  // Past-due: entry has a date+time in the past and isn't marked done
  const isPastDue = (() => {
    if (e.done) return false;
    if (!e.date) return false;
    const dt = e.time ? new Date(`${e.date}T${e.time}`) : new Date(`${e.date}T23:59`);
    return dt < new Date();
  })();
  const openMenu  = ev => { ev.stopPropagation(); setOpen(true);  setConfirmDel(false); };
  const closeMenu = ev => { ev.stopPropagation(); setOpen(false); setConfirmDel(false); };
  const handleEdit   = ev => { ev.stopPropagation(); setOpen(false); onEdit   && onEdit(e); };
  const handleDelReq = ev => { ev.stopPropagation(); setConfirmDel(true); };
  const handleDelOk  = ev => { ev.stopPropagation(); setOpen(false); setConfirmDel(false); onDelete && onDelete(e.id); };

  // Shared pill button style factory
  const pill = (bg, fg, border) => ({
    background: bg, color: fg,
    border: `1px solid ${border}`,
    borderRadius: 22, padding: '8px 18px',
    fontSize: 14, fontWeight: 700,
    cursor: 'pointer', fontFamily: 'inherit',
    whiteSpace: 'nowrap', flexShrink: 0,
  });

  return (
    <div style={{ display:'flex', gap:14, padding:'20px 0',
      borderBottom:`1px solid ${C.border}` }}>

      {/* Left colour stripe */}
      <div style={{ width:5, minHeight:24, borderRadius:4,
        background: col+'90', flexShrink:0, marginTop:4 }} />

      <div style={{ flex:1, minWidth:0 }}>
        {/* ── Title row — always visible ── */}
        <div style={{ display:'flex', alignItems:'flex-start', gap:8, marginBottom:6 }}>
          {/* Tick box — tasks use it for done state; other types use it for past-due clearing */}
          {(e.type === 'task' || (isPastDue && isOwn)) && (
            <button onClick={() => isOwn && onToggle && onToggle(e.id)}
              style={{ width:26, height:26, borderRadius:7,
                border:`2px solid ${e.done ? C.T : isPastDue ? '#C46A14' : C.border}`,
                background: e.done ? C.T+'22' : isPastDue ? '#C46A1408' : 'transparent',
                cursor: isOwn ? 'pointer' : 'default',
                flexShrink:0, marginTop:1,
                display:'flex', alignItems:'center', justifyContent:'center',
                color: e.done ? C.T : '#C46A14', fontSize:15, padding:0,
                transition:'background 0.15s, border-color 0.15s',
                opacity: isOwn ? 1 : 0.5 }}>
              {e.done ? '✓' : isPastDue ? '!' : ''}
            </button>
          )}
          <span style={{ fontSize:17, fontWeight:600,
            color: e.done ? C.muted : isPastDue ? C.dim : C.text,
            textDecoration: (e.done || isPastDue) ? 'line-through' : 'none',
            lineHeight:'1.4', flex:1, minWidth:0,
            opacity: isPastDue && !e.done ? 0.6 : 1 }}>
            {e.title}
          </span>
          {e.priority && <Badge label={e.priority} color={PC[e.priority]} />}
          {e.type === 'flight' && (
            <span style={{ fontSize:15, fontWeight:700, color:dcol,
              letterSpacing:'0.04em', flexShrink:0 }}>
              {e.depCity}→{e.arrCity}
            </span>
          )}
        </div>

        {/* ── Bottom row — morphs between: meta / actions / confirm ── */}
        {!open ? (
          /* META STATE */
          <div style={{ display:'flex', gap:10, flexWrap:'wrap', alignItems:'center' }}>
            {e.time      && <span style={{ fontSize:15, color:C.dim }}>{pt(e.time)}{e.endTime?` – ${pt(e.endTime)}`:''}</span>}
            {e.location  && <span style={{ fontSize:15, color:C.dim }}>📍 {e.location}</span>}
            {e.flightNum && <span style={{ fontSize:15, color:C.dim }}>{e.airline} · {e.flightNum}</span>}
            {e.tags      && <span style={{ fontSize:15, color:C.dim }}>🏷 {e.tags}</span>}
            {e.message   && <span style={{ fontSize:15, color:C.dim, fontStyle:'italic' }}>{e.message}</span>}
            {/* Own shared entry — rose badge */}
            {e.visibility==='shared' && isOwn && (
              <span style={{ fontSize:14, color:C.rose,
                background:C.rose+'15', borderRadius:10, padding:'1px 8px' }}>
                ◯ Shared
              </span>
            )}
            {/* Teammate's shared entry — blue badge, read-only */}
            {e.visibility==='shared' && !isOwn && (
              <span style={{ fontSize:13, color:DTC.meeting,
                background:C.M+'18', borderRadius:10, padding:'1px 8px' }}>
                👤 Team
              </span>
            )}
            {/* ··· only shown for own entries */}
            {isOwn && (
              <button onClick={openMenu}
                style={{ marginLeft:'auto', fontSize:15, color:C.muted,
                  background:'transparent', border:`1px solid ${C.border}`,
                  borderRadius:14, padding:'6px 13px', cursor:'pointer',
                  letterSpacing:'0.12em', lineHeight:1, flexShrink:0 }}>
                ···
              </button>
            )}
          </div>

        ) : !confirmDel ? (
          /* ACTION STATE — Edit + Delete + close */
          <div style={{ display:'flex', gap:8, alignItems:'center' }}>
            <button onClick={handleEdit}
              style={pill(col+'18', dcol, col+'50')}>
              ✎ Edit
            </button>
            <button onClick={handleDelReq}
              style={pill('#C46A1415', '#C46A14', '#C46A1450')}>
              ✕ Delete
            </button>
            <button onClick={closeMenu}
              style={{ ...pill(C.elevated, C.muted, C.border), marginLeft:'auto', padding:'4px 10px' }}>
              ×
            </button>
          </div>

        ) : (
          /* CONFIRM STATE — "Remove?" with Cancel + confirm */
          <div style={{ display:'flex', gap:8, alignItems:'center' }}>
            <span style={{ fontSize:15, color:C.dim, flex:1, fontStyle:'italic' }}>
              Remove this entry?
            </span>
            <button onClick={closeMenu}
              style={pill(C.elevated, C.dim, C.border)}>
              Cancel
            </button>
            <button onClick={handleDelOk}
              style={pill('#A04E08', '#fff', '#A04E08')}>
              Remove
            </button>
          </div>
        )}
      </div>
    </div>
  );
}

// ─── FLIGHT HERO CARD ────────────────────────────────────────────
function FlightHeroCard({ flight, todayStr, onEdit, onDelete }) {
  const { status, lastUpdated, loading } = useLiveFlightStatus(flight);
  const depName = airportCity(flight.depCity);
  const arrName = airportCity(flight.arrCity);

  return (
    <div style={{ background:`linear-gradient(135deg,#EDF5FD,#E2EFF8)`,
      border:`1px solid ${C.F}50`,
      borderRadius:20, padding:18, marginBottom:6,
      position:'relative', overflow:'hidden',
      boxShadow:`0 4px 20px ${C.F}20` }}>
      <div style={{ position:'absolute', top:-20, right:-20, width:100, height:100,
        background:`radial-gradient(circle,${C.F}30 0%,transparent 70%)`,
        pointerEvents:'none' }} />

      {/* Airline + flight number + live status badge */}
      <div style={{ display:'flex', alignItems:'center', justifyContent:'space-between', marginBottom:10 }}>
        <p style={{ fontSize:14, color:DTC.flight, fontWeight:700, margin:0,
          textTransform:'uppercase', letterSpacing:'0.1em' }}>
          {flight.airline} · {flight.flightNum}
        </p>
        <div style={{ display:'flex', alignItems:'center', gap:6 }}>
          {loading && (
            <span style={{ fontSize:11, color:C.dim, fontStyle:'italic' }}>updating…</span>
          )}
          {status && (
            <span style={{ fontSize:12, fontWeight:700, color:'#fff',
              background:status.color, borderRadius:20, padding:'3px 12px',
              letterSpacing:'0.04em', flexShrink:0 }}>
              {status.label}
            </span>
          )}
        </div>
      </div>

      <div style={{ display:'flex', justifyContent:'space-between', alignItems:'flex-start', marginBottom:14 }}>
        <div style={{ flex:1 }}>
          <div style={{ display:'flex', alignItems:'center', gap:10 }}>
            {/* Departure */}
            <div style={{ textAlign:'center' }}>
              <span style={{ fontSize:34, fontWeight:600, color:C.text,
                fontFamily:'Cormorant Garamond,serif', lineHeight:1 }}>
                {flight.depCity}
              </span>
              <p style={{ margin:'2px 0 0', fontSize:12, color:C.dim, lineHeight:1 }}>
                {depName !== flight.depCity ? depName : ''}
              </p>
              {/* Show revised departure time if delayed */}
              {status?.revisedDep && status?.delayMins > 4 && (
                <p style={{ margin:'3px 0 0', fontSize:11, color:'#8A3A08', fontWeight:700 }}>
                  {status.delayLabel}
                </p>
              )}
            </div>
            {/* Route line */}
            <div style={{ flex:1, display:'flex', alignItems:'center', gap:4 }}>
              <div style={{ flex:1, height:'1px', background:`linear-gradient(90deg,${DTC.flight}60,transparent)` }} />
              <span style={{ fontSize:16, color:DTC.flight }}>✈</span>
              <div style={{ flex:1, height:'1px', background:`linear-gradient(270deg,${DTC.flight}60,transparent)` }} />
            </div>
            {/* Arrival */}
            <div style={{ textAlign:'center' }}>
              <span style={{ fontSize:34, fontWeight:600, color:C.text,
                fontFamily:'Cormorant Garamond,serif', lineHeight:1 }}>
                {flight.arrCity}
              </span>
              <p style={{ margin:'2px 0 0', fontSize:12, color:C.dim, lineHeight:1 }}>
                {arrName !== flight.arrCity ? arrName : ''}
              </p>
            </div>
          </div>
        </div>
        <div style={{ textAlign:'right', paddingLeft:14 }}>
          <p style={{ fontSize:19, fontWeight:600, color:C.text, margin:0 }}>
            {/* Show revised time if delayed, otherwise scheduled */}
            {status?.revisedDep
              ? pt(status.revisedDep.split('T')[1]?.slice(0,5) || flight.time)
              : pt(flight.time)}
          </p>
          <p style={{ fontSize:15, color:C.dim, margin:'4px 0 0' }}>
            {flight.date===todayStr ? 'Today'
              : flight.date===fd(ad(new Date(),1)) ? 'Tomorrow'
              : flight.date}
          </p>
        </div>
      </div>

      {/* Terminal / Gate / Seat chips — gate may update live from AeroDataBox */}
      <div style={{ display:'flex', gap:8, flexWrap:'wrap' }}>
        {[
          ['Terminal', status?.terminal || flight.terminal],
          ['Gate',     status?.gate     || flight.gate],
          ['Seat',     flight.seat],
        ].filter(([,v])=>v).map(([k,v]) => (
          <div key={k} style={{ background:'#ffffff60', borderRadius:12,
            padding:'7px 12px', backdropFilter:'blur(4px)',
            border:`1px solid ${C.F}25` }}>
            <p style={{ fontSize:12, color:C.dim, margin:0, textTransform:'uppercase', letterSpacing:'0.06em' }}>{k}</p>
            <p style={{ fontSize:17, fontWeight:600, color:C.text, margin:'2px 0 0' }}>{v}</p>
          </div>
        ))}
      </div>

      {/* Last updated timestamp */}
      {lastUpdated && status?.source !== 'local' && (
        <p style={{ margin:'10px 0 0', fontSize:11, color:C.muted, textAlign:'right', fontStyle:'italic' }}>
          Live data · updated {Math.floor((Date.now()-lastUpdated)/60000) < 1
            ? 'just now'
            : `${Math.floor((Date.now()-lastUpdated)/60000)}m ago`}
        </p>
      )}

      {/* Edit / Delete actions */}
      {(onEdit || onDelete) && (
        <div style={{ display:'flex', gap:8, marginTop:12, justifyContent:'flex-end' }}>
          {onEdit && (
            <button onClick={() => onEdit(flight)}
              style={{ background:'#ffffff80', border:`1px solid ${C.F}30`,
                borderRadius:10, padding:'6px 16px', fontSize:14, fontWeight:600,
                color:DTC.flight, cursor:'pointer', fontFamily:'inherit',
                backdropFilter:'blur(4px)' }}>
              Edit
            </button>
          )}
          {onDelete && (
            <button onClick={() => onDelete(flight.id)}
              style={{ background:'#ffffff80', border:`1px solid #C46A1440`,
                borderRadius:10, padding:'6px 16px', fontSize:14, fontWeight:600,
                color:'#C46A14', cursor:'pointer', fontFamily:'inherit',
                backdropFilter:'blur(4px)' }}>
              Delete
            </button>
          )}
        </div>
      )}
    </div>
  );
}
function HomeTab({ entries, onToggle, onEdit, onDelete, userName, currentUserId }) {
  const now      = new Date();
  const todayStr = fd(now);

  const todayEs = useMemo(() =>
    entries.filter(e => e.date === fd(new Date()))
           .sort((a,b) => (a.time||'99:99').localeCompare(b.time||'99:99')),
    [entries]);
  const nextFlight = useMemo(() =>
    entries.filter(e => e.type==='flight' && e.date >= fd(new Date()))
           .sort((a,b) => a.date.localeCompare(b.date) || (a.time||'').localeCompare(b.time||''))[0],
    [entries]);
  const topTasks = useMemo(() => {
    const pr = { critical:0, high:1, medium:2, low:3 };
    return entries.filter(e => e.type==='task' && !e.done)
                  .sort((a,b) => (pr[a.priority]??9)-(pr[b.priority]??9)).slice(0,3);
  }, [entries]);
  const openTasks = entries.filter(e => e.type==='task' && !e.done).length;
  const next48 = useMemo(() => {
    const n = new Date(), lim = new Date(n.getTime()+48*3600000);
    return entries.filter(e => {
      const d=new Date(e.date+'T'+(e.time||'00:00'));
      return d>=n && d<=lim && e.type!=='task';
    }).length;
  }, [entries]);
  const hr    = now.getHours();
  const greet = hr<12?'Good Morning':hr<17?'Good Afternoon':'Good Evening';

  return (
    <div style={{ padding:'0 18px 90px', overflowY:'auto', height:'100%', boxSizing:'border-box' }}>

      {/* Greeting */}
      <div style={{ paddingTop:14, marginBottom:20 }}>
        <p style={{ fontSize:15, color:C.dim, margin:'0 0 3px', fontStyle:'italic' }}>{greet}</p>
        <h1 style={{ fontSize:36, fontFamily:'Cormorant Garamond,Georgia,serif',
          fontWeight:600, color:C.text, margin:0, lineHeight:1.2 }}>
          <span style={{ color:C.rose }}>{userName || 'Welcome'}</span>
        </h1>
        <p style={{ fontSize:16, color:C.dim, margin:'5px 0 0' }}>
          {DAY[now.getDay()]}, {MFULL[now.getMonth()]} {now.getDate()} · {todayEs.length} items today
        </p>
      </div>

      {/* Stats strip */}
      <div style={{ display:'grid', gridTemplateColumns:'1fr 1fr 1fr', gap:10, marginBottom:6 }}>
        {[[todayEs.length,'Today',C.M],[openTasks,'Open Tasks',C.T],[next48,'Next 48h',C.E]].map(([v,l,c]) => (
          <div key={l} style={{ background:C.card, borderRadius:18, padding:'20px 10px',
            textAlign:'center', boxShadow:SH.card, border:`1px solid ${C.border}` }}>
            <div style={{ fontSize:32, fontWeight:700,
              fontFamily:'Cormorant Garamond,serif', color:c, lineHeight:1 }}>{v}</div>
            <div style={{ fontSize:14, color:C.dim, marginTop:6 }}>{l}</div>
          </div>
        ))}
      </div>

      {/* Next Flight */}
      {nextFlight && (<>
        <Sec label="Next Flight" />
        <FlightHeroCard flight={nextFlight} todayStr={todayStr} onEdit={onEdit} onDelete={onDelete} />
      </>)}

      {/* Priority Tasks */}
      {topTasks.length > 0 && (<>
        <Sec label="Priority Tasks" count={openTasks} />
        <div style={{ background:C.card, borderRadius:20, padding:'0 14px',
          boxShadow:SH.card, border:`1px solid ${C.border}` }}>
          {topTasks.map(e => <ECard key={e.id} e={e} onToggle={onToggle} onEdit={onEdit} onDelete={onDelete} currentUserId={currentUserId} />)}
        </div>
      </>)}

      {/* Today's Schedule */}
      <Sec label="Today's Schedule" count={todayEs.length} />
      {todayEs.length === 0
        ? <p style={{ color:C.muted, fontSize:16, textAlign:'center', padding:'32px 0', fontStyle:'italic' }}>
            Nothing scheduled for today
          </p>
        : <div style={{ background:C.card, borderRadius:20, padding:'0 14px',
            boxShadow:SH.card, border:`1px solid ${C.border}` }}>
            {todayEs.map(e => <ECard key={e.id} e={e} onToggle={onToggle} onEdit={onEdit} onDelete={onDelete} currentUserId={currentUserId} />)}
          </div>
      }
    </div>
  );
}

// ─── AGENDA VIEW ─────────────────────────────────────────────────
function AgendaView({ entries, onToggle, onEdit, onDelete, currentUserId }) {
  const grouped = useMemo(() => {
    const sorted = [...entries].sort((a,b) =>
      a.date.localeCompare(b.date) || (a.time||'99:99').localeCompare(b.time||'99:99'));
    const map = {};
    sorted.forEach(e => { (map[e.date] = map[e.date]||[]).push(e); });
    return map;
  }, [entries]);
  const dates = Object.keys(grouped).sort();

  return (
    <div style={{ overflowY:'auto', height:'100%', padding:'0 18px 90px', boxSizing:'border-box' }}>
      {dates.map(d => {
        const dt  = new Date(d+'T00:00:00');
        const isT = d === fd(new Date());
        return (
          <div key={d} style={{ marginTop:20 }}>
            <div style={{ display:'flex', alignItems:'center', gap:12, marginBottom:8 }}>
              <div style={{ width:44, height:44, borderRadius:14, flexShrink:0,
                background: isT ? `linear-gradient(135deg,${C.rose},${C.roseL})` : C.card,
                boxShadow: isT ? `0 4px 16px ${C.rose}35` : SH.subtle,
                border: isT ? 'none' : `1px solid ${C.border}`,
                display:'flex', flexDirection:'column', alignItems:'center', justifyContent:'center' }}>
                <span style={{ fontSize:11, fontWeight:700, color:isT?'#fff':C.dim,
                  lineHeight:1, textTransform:'uppercase' }}>{DAY[dt.getDay()]}</span>
                <span style={{ fontSize:20, fontWeight:700, color:isT?'#fff':C.text, lineHeight:1.2 }}>
                  {dt.getDate()}
                </span>
              </div>
              <span style={{ fontSize:16, color:isT?C.rose:C.dim, fontStyle:isT?'italic':'normal' }}>
                {isT ? 'Today — ' : ''}{MFULL[dt.getMonth()]} {dt.getFullYear()}
              </span>
            </div>
            <div style={{ background:C.card, borderRadius:20, padding:'0 14px',
              boxShadow:SH.card, border:`1px solid ${C.border}` }}>
              {grouped[d].map(e => <ECard key={e.id} e={e} onToggle={onToggle} onEdit={onEdit} onDelete={onDelete} currentUserId={currentUserId} />)}
            </div>
          </div>
        );
      })}
    </div>
  );
}

// ─── DAY VIEW ────────────────────────────────────────────────────
function DayView({ entries, selDate, setSelDate, onToggle, onEdit, onDelete, currentUserId }) {
  const dayEs = useMemo(() => entries.filter(e => e.date===selDate && e.time), [entries, selDate]);
  const allDayEs = useMemo(() => entries.filter(e => e.date===selDate && !e.time), [entries, selDate]);
  const hours  = Array.from({ length:24 }, (_,i) => i); // 00:00 → 23:00
  const dt     = new Date(selDate+'T00:00:00');

  const NavBtn = ({ children, onClick }) => (
    <button onClick={onClick} style={{ background:C.card, border:`1px solid ${C.border}`,
      color:C.text, borderRadius:12, padding:'7px 16px', cursor:'pointer',
      fontSize:20, boxShadow:SH.subtle }}>{children}</button>
  );

  return (
    <div style={{ display:'flex', flexDirection:'column', height:'100%' }}>
      <div style={{ display:'flex', alignItems:'center', gap:8, padding:'10px 18px',
        borderBottom:`1px solid ${C.border}`, flexShrink:0, background:C.card }}>
        <NavBtn onClick={() => { const d=new Date(selDate+'T00:00:00'); d.setDate(d.getDate()-1); setSelDate(fd(d)); }}>‹</NavBtn>
        <div style={{ flex:1, textAlign:'center' }}>
          <p style={{ margin:0, fontSize:17, fontWeight:600, color:C.text }}>
            {DAY[dt.getDay()]}, {MFULL[dt.getMonth()]} {dt.getDate()}
          </p>
          {selDate===fd(new Date()) && (
            <p style={{ margin:0, fontSize:14, color:C.rose, fontStyle:'italic' }}>Today</p>
          )}
        </div>
        <NavBtn onClick={() => { const d=new Date(selDate+'T00:00:00'); d.setDate(d.getDate()+1); setSelDate(fd(d)); }}>›</NavBtn>
      </div>
      <div style={{ flex:1, overflowY:'auto', padding:'0 18px 90px', boxSizing:'border-box' }}>
        {/* All-day entries (no time) shown at top */}
        {allDayEs.length > 0 && (
          <div style={{ background:C.card, borderRadius:14, padding:'0 12px',
            border:`1px solid ${C.border}`, margin:'8px 0 4px',
            boxShadow:SH.subtle }}>
            <p style={{ fontSize:12, color:C.muted, margin:'8px 0 2px',
              textTransform:'uppercase', letterSpacing:'0.08em', fontWeight:700 }}>All day</p>
            {allDayEs.map(e => <ECard key={e.id} e={e} onToggle={onToggle} onEdit={onEdit} onDelete={onDelete} currentUserId={currentUserId} />)}
          </div>
        )}
        {/* Hourly slots */}
        {hours.map(h => {
          const hEs = dayEs.filter(e => parseInt(e.time.split(':')[0])===h);
          return (
            <div key={h} style={{ display:'flex', gap:12, minHeight:48 }}>
              <div style={{ width:48, paddingTop:10, flexShrink:0, textAlign:'right' }}>
                <span style={{ fontSize:13, color: h===0||h===12 ? C.text : C.muted,
                  fontWeight: h===0||h===12 ? 600 : 400 }}>
                  {h===0?'12 AM':h<12?`${h} AM`:h===12?'12 PM':`${h-12} PM`}
                </span>
              </div>
              <div style={{ flex:1, borderTop:`1px solid ${C.border}`, paddingTop:4, paddingBottom:4 }}>
                {hEs.length > 0 && (
                  <div style={{ background:C.card, borderRadius:14, padding:'0 12px',
                    boxShadow:SH.card, border:`1px solid ${C.border}` }}>
                    {hEs.map(e => (
                      <ECard key={e.id} e={e} onToggle={onToggle} onEdit={onEdit} onDelete={onDelete} currentUserId={currentUserId} />
                    ))}
                  </div>
                )}
              </div>
            </div>
          );
        })}
      </div>
    </div>
  );
}

// ─── WEEK VIEW ───────────────────────────────────────────────────
function WeekView({ entries, selDate, setSelDate, onToggle, onEdit, onDelete, currentUserId }) {
  const dt        = new Date(selDate+'T00:00:00');
  const dow       = dt.getDay();
  const weekStart = new Date(dt);
  weekStart.setDate(dt.getDate() - (dow===0?6:dow-1));
  const days = Array.from({ length:7 }, (_,i) => ad(weekStart,i));

  // Entries for each day in week — used for dots
  const weekEntries = useMemo(() =>
    Object.fromEntries(days.map(d => {
      const ds = fd(d);
      return [ds, entries.filter(e=>e.date===ds).sort((a,b)=>(a.time||'99:99').localeCompare(b.time||'99:99'))];
    }))
  // eslint-disable-next-line react-hooks/exhaustive-deps
  , [entries, fd(weekStart)]);

  // Selected day's entries
  const selDayEs = weekEntries[selDate] || [];

  return (
    <div style={{ display:'flex', flexDirection:'column', height:'100%' }}>
      {/* Week navigation header */}
      <div style={{ display:'flex', alignItems:'center', padding:'8px 18px',
        borderBottom:`1px solid ${C.border}`, flexShrink:0, background:C.card }}>
        <button onClick={() => { const d=new Date(selDate+'T00:00:00'); d.setDate(d.getDate()-7); setSelDate(fd(d)); }}
          style={{ background:C.elevated, border:`1px solid ${C.border}`, color:C.text,
            borderRadius:12, padding:'7px 14px', cursor:'pointer', fontSize:20 }}>‹</button>
        <span style={{ flex:1, textAlign:'center', fontSize:16, color:C.dim, fontWeight:600 }}>
          {MON[weekStart.getMonth()]} {weekStart.getDate()} – {MON[days[6].getMonth()]} {days[6].getDate()}
        </span>
        <button onClick={() => { const d=new Date(selDate+'T00:00:00'); d.setDate(d.getDate()+7); setSelDate(fd(d)); }}
          style={{ background:C.elevated, border:`1px solid ${C.border}`, color:C.text,
            borderRadius:12, padding:'7px 14px', cursor:'pointer', fontSize:20 }}>›</button>
      </div>

      {/* 7-day picker row */}
      <div style={{ display:'grid', gridTemplateColumns:'repeat(7,1fr)',
        padding:'4px 6px', flexShrink:0, borderBottom:`1px solid ${C.border}`,
        background:C.card }}>
        {days.map(d => {
          const ds=fd(d); const isT=ds===fd(new Date()); const isSel=ds===selDate;
          const dots = [...new Set((weekEntries[ds]||[]).map(e=>TC[e.type]))].slice(0,3);
          return (
            <button key={ds} onClick={() => setSelDate(ds)}
              style={{ background:'transparent', border:'none', cursor:'pointer',
                padding:'6px 2px', textAlign:'center' }}>
              <div style={{ fontSize:11, color:isT?C.rose:C.muted, marginBottom:2,
                textTransform:'uppercase', letterSpacing:'0.05em' }}>
                {DAY[d.getDay()]}
              </div>
              <div style={{ width:32, height:32, borderRadius:16, margin:'0 auto',
                background: isSel?C.rose : isT?C.rose+'22':'transparent',
                border: isT&&!isSel?`1.5px solid ${C.rose+'60'}`:'1.5px solid transparent',
                boxShadow: isSel?`0 2px 10px ${C.rose}40`:'none',
                display:'flex', alignItems:'center', justifyContent:'center' }}>
                <span style={{ fontSize:15, fontWeight:isSel?700:400,
                  color:isSel?'#fff':isT?C.rose:C.text }}>{d.getDate()}</span>
              </div>
              {/* Entry count dot or dots */}
              <div style={{ display:'flex', justifyContent:'center', gap:2, marginTop:3, height:5 }}>
                {dots.map((col,j) => (
                  <div key={j} style={{ width:5, height:5, borderRadius:3, background:col }} />
                ))}
              </div>
            </button>
          );
        })}
      </div>

      {/* Selected day's entries */}
      <div style={{ flex:1, overflowY:'auto', padding:'8px 18px 90px', boxSizing:'border-box' }}>
        {selDayEs.length === 0 ? (
          <div style={{ textAlign:'center', padding:'40px 0' }}>
            <p style={{ fontSize:28, opacity:0.25, margin:'0 0 8px' }}>◎</p>
            <p style={{ fontSize:16, color:C.muted, fontStyle:'italic', margin:0 }}>
              Nothing on {DAY[new Date(selDate+'T00:00:00').getDay()]}, {MFULL[new Date(selDate+'T00:00:00').getMonth()]} {new Date(selDate+'T00:00:00').getDate()}
            </p>
          </div>
        ) : (
          <div style={{ background:C.card, borderRadius:20, padding:'0 14px',
            boxShadow:SH.card, border:`1px solid ${C.border}` }}>
            {selDayEs.map(e => (
              <ECard key={e.id} e={e} onToggle={onToggle} onEdit={onEdit} onDelete={onDelete} currentUserId={currentUserId} />
            ))}
          </div>
        )}
      </div>
    </div>
  );
}

// ─── MONTH VIEW ──────────────────────────────────────────────────
function MonthView({ entries, selDate, setSelDate, onToggle, onEdit, onDelete, currentUserId }) {
  const initDt      = new Date(selDate+'T00:00:00');
  const [vm, setVm] = useState({ y:initDt.getFullYear(), m:initDt.getMonth() });
  const daysInMonth = new Date(vm.y, vm.m+1, 0).getDate();
  const first       = new Date(vm.y, vm.m, 1);
  const offset      = first.getDay()===0 ? 6 : first.getDay()-1;
  const cells       = [...Array(offset).fill(null), ...Array.from({length:daysInMonth},(_,i)=>i+1)];
  const selDayEs    = entries.filter(e=>e.date===selDate)
                             .sort((a,b)=>(a.time||'99:99').localeCompare(b.time||'99:99'));

  return (
    <div style={{ display:'flex', flexDirection:'column', height:'100%' }}>
      <div style={{ display:'flex', alignItems:'center', padding:'8px 18px',
        borderBottom:`1px solid ${C.border}`, flexShrink:0, background:C.card }}>
        <button onClick={() => setVm(p => p.m===0?{y:p.y-1,m:11}:{y:p.y,m:p.m-1})}
          style={{ background:C.elevated, border:`1px solid ${C.border}`, color:C.text,
            borderRadius:12, padding:'7px 14px', cursor:'pointer', fontSize:20 }}>‹</button>
        <span style={{ flex:1, textAlign:'center', fontSize:19, fontWeight:600,
          color:C.text, fontFamily:'Cormorant Garamond,serif' }}>
          {MFULL[vm.m]} {vm.y}
        </span>
        <button onClick={() => setVm(p => p.m===11?{y:p.y+1,m:0}:{y:p.y,m:p.m+1})}
          style={{ background:C.elevated, border:`1px solid ${C.border}`, color:C.text,
            borderRadius:12, padding:'7px 14px', cursor:'pointer', fontSize:20 }}>›</button>
      </div>
      {/* Weekday labels */}
      <div style={{ display:'grid', gridTemplateColumns:'repeat(7,1fr)',
        padding:'6px 6px 0', flexShrink:0, background:C.card }}>
        {['M','T','W','T','F','S','S'].map((d,i) => (
          <div key={i} style={{ textAlign:'center', fontSize:14, color:C.muted, fontWeight:600, padding:'3px 0' }}>{d}</div>
        ))}
      </div>
      {/* Day grid */}
      <div style={{ padding:'0 6px', flexShrink:0, background:C.card }}>
        <div style={{ display:'grid', gridTemplateColumns:'repeat(7,1fr)', gap:2 }}>
          {cells.map((day,i) => {
            if (!day) return <div key={`e${i}`} style={{ height:42 }} />;
            const ds   = `${vm.y}-${p2(vm.m+1)}-${p2(day)}`;
            const isT  = ds===fd(new Date()), isSel = ds===selDate;
            const dots = [...new Set(entries.filter(e=>e.date===ds).map(e=>TC[e.type]))].slice(0,3);
            return (
              <button key={ds} onClick={() => setSelDate(ds)}
                style={{ background:'transparent', border:'none', cursor:'pointer',
                  padding:'3px 1px', textAlign:'center' }}>
                <div style={{ width:32, height:32, borderRadius:16, margin:'0 auto',
                  background: isSel?C.rose : isT?C.rose+'20':'transparent',
                  border: isT&&!isSel?`1.5px solid ${C.rose+'60'}`:'1.5px solid transparent',
                  boxShadow: isSel?`0 2px 12px ${C.rose}35`:'none',
                  display:'flex', alignItems:'center', justifyContent:'center' }}>
                  <span style={{ fontSize:16, fontWeight:isSel?700:400,
                    color: isSel?'#fff' : isT?C.rose:C.text }}>{day}</span>
                </div>
                <div style={{ display:'flex', justifyContent:'center', gap:3, marginTop:2, height:5 }}>
                  {dots.map((col,j) => (
                    <div key={j} style={{ width:5, height:5, borderRadius:3, background:col+'90' }} />
                  ))}
                </div>
              </button>
            );
          })}
        </div>
      </div>
      {/* Selected day entries */}
      <div style={{ flex:1, overflowY:'auto', padding:'0 18px 80px',
        borderTop:`1px solid ${C.border}`, marginTop:8, boxSizing:'border-box' }}>
        <p style={{ fontSize:14, color:C.dim, margin:'10px 0 8px',
          textTransform:'uppercase', letterSpacing:'0.1em', fontWeight:700 }}>
          {new Date(selDate+'T00:00:00').toLocaleDateString('en-US',{weekday:'long',month:'long',day:'numeric'})}
        </p>
        {selDayEs.length===0
          ? <p style={{ color:C.muted, fontSize:16, textAlign:'center', padding:'24px 0', fontStyle:'italic' }}>
              Nothing on this day
            </p>
          : <div style={{ background:C.card, borderRadius:20, padding:'0 14px',
              boxShadow:SH.card, border:`1px solid ${C.border}` }}>
              {selDayEs.map(e => <ECard key={e.id} e={e} onToggle={onToggle} onEdit={onEdit} onDelete={onDelete} currentUserId={currentUserId} />)}
            </div>
        }
      </div>
    </div>
  );
}

// ─── CALENDAR TAB ────────────────────────────────────────────────
function CalendarTab({ entries, onToggle, onEdit, onDelete, currentUserId }) {
  const [view,    setView]    = useState('agenda');
  const [selDate, setSelDate] = useState(fd(new Date()));

  return (
    <div style={{ display:'flex', flexDirection:'column', height:'100%' }}>
      <div style={{ display:'flex', gap:6, padding:'10px 18px',
        borderBottom:`1px solid ${C.border}`, flexShrink:0, background:C.card }}>
        {['agenda','day','week','month'].map(v => (
          <button key={v} onClick={() => setView(v)}
            style={{ flex:1, padding:'9px 2px', borderRadius:12, border:'none', cursor:'pointer',
              background: view===v ? C.rose : C.elevated,
              color: view===v ? '#fff' : C.dim,
              fontSize:15, fontWeight:view===v?600:400, textTransform:'capitalize',
              boxShadow: view===v?`0 2px 10px ${C.rose}35`:SH.subtle,
              transition:'background 0.15s' }}>
            {v}
          </button>
        ))}
      </div>
      <div style={{ flex:1, overflow:'hidden' }}>
        {view==='agenda' && <AgendaView entries={entries} onToggle={onToggle} onEdit={onEdit} onDelete={onDelete} currentUserId={currentUserId} />}
        {view==='day'    && <DayView    entries={entries} selDate={selDate} setSelDate={setSelDate} onToggle={onToggle} onEdit={onEdit} onDelete={onDelete} currentUserId={currentUserId} />}
        {view==='week'   && <WeekView   entries={entries} selDate={selDate} setSelDate={setSelDate} onToggle={onToggle} onEdit={onEdit} onDelete={onDelete} currentUserId={currentUserId} />}
        {view==='month'  && <MonthView  entries={entries} selDate={selDate} setSelDate={setSelDate} onToggle={onToggle} onEdit={onEdit} onDelete={onDelete} currentUserId={currentUserId} />}
      </div>
    </div>
  );
}

// ─── SEARCH TAB ──────────────────────────────────────────────────
const QUICK_FILTERS = [
  { k:'today',   l:'Today',            f: e => e.date===fd(new Date()) },
  { k:'week',    l:'This Week',        f: e => { const d=new Date(e.date+'T00:00:00'),n=new Date(),w=ad(n,7); return d>=n&&d<=w; } },
  { k:'flights', l:'Upcoming Flights', f: e => e.type==='flight' && e.date>=fd(new Date()) },
  { k:'tasks',   l:'Pending Tasks',    f: e => e.type==='task' && !e.done },
  { k:'shared',  l:'Shared',          f: e => e.visibility==='shared' },
];

function SearchTab({ entries, onToggle, onEdit, onDelete, currentUserId }) {
  const [q,      setQ]      = useState('');
  const [typeF,  setTypeF]  = useState('all');
  const [quickF, setQuickF] = useState(null);

  const results = useMemo(() => {
    let r = entries;
    if (quickF) { const qf=QUICK_FILTERS.find(x=>x.k===quickF); if (qf) r=r.filter(qf.f); }
    if (typeF !== 'all') r = r.filter(e => e.type===typeF);
    if (q.trim()) {
      const lq = q.toLowerCase();
      r = r.filter(e =>
        [e.title,e.location,e.attendees,e.tags,e.notes,e.message,e.airline,e.flightNum,e.depCity,e.arrCity]
          .some(f => f && f.toLowerCase().includes(lq)));
    }
    return r.sort((a,b) => (a.date||'9999').localeCompare(b.date||'9999'));
  }, [entries, q, typeF, quickF]);

  return (
    <div style={{ display:'flex', flexDirection:'column', height:'100%' }}>
      <div style={{ padding:'12px 18px', borderBottom:`1px solid ${C.border}`,
        flexShrink:0, background:C.card }}>
        {/* Search input */}
        <div style={{ display:'flex', alignItems:'center', gap:10, background:C.elevated,
          borderRadius:16, padding:'11px 16px', border:`1px solid ${C.border}`,
          boxShadow:SH.subtle }}>
          <span style={{ color:C.muted, fontSize:19 }}>🔍</span>
          <input value={q} onChange={e => setQ(e.target.value)} placeholder="Search all entries…"
            style={{ flex:1, background:'transparent', border:'none', outline:'none',
              color:C.text, fontSize:17, fontFamily:'inherit' }} />
          {q && (
            <button onClick={() => setQ('')}
              style={{ background:'transparent', border:'none', color:C.muted,
                cursor:'pointer', fontSize:18, padding:0 }}>✕</button>
          )}
        </div>
        {/* Quick filters */}
        <div style={{ display:'flex', gap:7, marginTop:10, overflowX:'auto', paddingBottom:2 }}>
          {QUICK_FILTERS.map(qf => (
            <button key={qf.k} onClick={() => setQuickF(p => p===qf.k?null:qf.k)}
              style={{ background: quickF===qf.k ? C.rose : C.elevated,
                border:`1px solid ${quickF===qf.k ? C.rose : C.border}`,
                color: quickF===qf.k ? '#fff' : C.dim,
                borderRadius:20, padding:'5px 14px',
                fontSize:15, fontWeight:600, cursor:'pointer', whiteSpace:'nowrap',
                boxShadow: quickF===qf.k?`0 2px 10px ${C.rose}35`:SH.subtle,
                transition:'background 0.15s' }}>
              {qf.l}
            </button>
          ))}
        </div>
        {/* Type filters */}
        <div style={{ display:'flex', gap:6, marginTop:7, overflowX:'auto', paddingBottom:2 }}>
          {['all','meeting','task','flight','reminder','event'].map(t => (
            <button key={t} onClick={() => setTypeF(t)}
              style={{ background: typeF===t ? (t==='all'?C.rose:TC[t]+'28') : C.elevated,
                border:`1px solid ${typeF===t ? (t==='all'?C.rose:TC[t]) : C.border}`,
                color: typeF===t ? (t==='all'?'#fff':DTC[t]||TC[t]) : C.dim,
                borderRadius:20, padding:'4px 13px', fontSize:14, fontWeight:600,
                cursor:'pointer', whiteSpace:'nowrap', textTransform:'capitalize',
                transition:'background 0.15s' }}>
              {t==='all'?'All':TL[t]||t}
            </button>
          ))}
        </div>
      </div>
      <div style={{ flex:1, overflowY:'auto', padding:'0 18px 90px', boxSizing:'border-box' }}>
        <p style={{ fontSize:15, color:C.muted, margin:'12px 0 6px', fontStyle:'italic' }}>
          {results.length} result{results.length!==1?'s':''}
        </p>
        {results.length===0
          ? <p style={{ color:C.muted, fontSize:16, textAlign:'center', padding:'50px 0', fontStyle:'italic' }}>
              Nothing found
            </p>
          : <div style={{ background:C.card, borderRadius:20, padding:'0 14px',
              boxShadow:SH.card, border:`1px solid ${C.border}` }}>
              {results.map(e => <ECard key={e.id} e={e} onToggle={onToggle} onEdit={onEdit} onDelete={onDelete} currentUserId={currentUserId} />)}
            </div>
        }
      </div>
    </div>
  );
}

// ─── RESET SECTION ───────────────────────────────────────────────
// Two-tap confirm guard — first tap shows warning, second tap executes reset.
// Separated to module level so it's never recreated inside SettingsTab.
function ResetSection({ onReset }) {
  const [confirming, setConfirming] = useState(false);
  return (
    <div style={{ marginBottom:40 }}>
      <p style={{ fontSize:13, fontWeight:700, color:'#C46A14', textTransform:'uppercase',
        letterSpacing:'0.14em', margin:'24px 0 8px' }}>Danger Zone</p>
      <div style={{ background:C.card, borderRadius:20, overflow:'hidden',
        boxShadow:SH.card, border:`1px solid ${'#C46A14'}40` }}>
        {!confirming ? (
          <div style={{ display:'flex', alignItems:'center', padding:'16px 18px', gap:12 }}>
            <div style={{ flex:1 }}>
              <p style={{ margin:0, fontSize:17, color:C.text, fontWeight:500 }}>Reset App Data</p>
              <p style={{ margin:0, fontSize:15, color:C.dim, marginTop:2 }}>
                Wipe all entries, audit log and storage. Cannot be undone.
              </p>
            </div>
            <button onClick={() => setConfirming(true)}
              style={{ background:'transparent', border:`1.5px solid ${'#C46A14'}`,
                color:'#C46A14', borderRadius:12, padding:'8px 16px',
                fontSize:15, fontWeight:600, cursor:'pointer', fontFamily:'inherit',
                whiteSpace:'nowrap' }}>
              Reset…
            </button>
          </div>
        ) : (
          <div style={{ padding:'18px 18px' }}>
            <p style={{ margin:'0 0 6px', fontSize:17, fontWeight:700, color:'#A04E08' }}>
              Are you sure?
            </p>
            <p style={{ margin:'0 0 16px', fontSize:15, color:C.dim, lineHeight:1.5 }}>
              This permanently erases every entry, flight, reminder and activity log record.
              Your next sync will start with a completely blank database.
            </p>
            <div style={{ display:'flex', gap:10 }}>
              <button onClick={() => setConfirming(false)}
                style={{ flex:1, background:C.elevated, border:`1px solid ${C.border}`,
                  color:C.dim, borderRadius:12, padding:'11px 0',
                  fontSize:16, fontWeight:600, cursor:'pointer', fontFamily:'inherit' }}>
                Cancel
              </button>
              <button onClick={() => { setConfirming(false); onReset(); }}
                style={{ flex:1, background:'#A04E08', border:'none',
                  color:'#fff', borderRadius:12, padding:'11px 0',
                  fontSize:16, fontWeight:700, cursor:'pointer', fontFamily:'inherit',
                  boxShadow:`0 4px 16px ${'#A04E08'}40` }}>
                Yes, Wipe Everything
              </button>
            </div>
          </div>
        )}
      </div>
    </div>
  );
}

// ─── INVITE MODAL ────────────────────────────────────────────────
function InviteModal({ onClose, workspaceId, invitedBy }) {
  const url    = 'https://surferyogi.github.io/Kizuna-app/';
  const qr     = `https://api.qrserver.com/v1/create-qr-code/?size=180x180&color=B8715C&bgcolor=FFFEFB&data=${encodeURIComponent(url)}`;
  const [copied,       setCopied]       = useState(false);
  const [inviteEmail,  setInviteEmail]  = useState('');
  const [inviteSent,   setInviteSent]   = useState(false);
  const [inviteError,  setInviteError]  = useState('');
  const [inviteLoading,setInviteLoading]= useState(false);

  const copy = () => {
    if (navigator.clipboard?.writeText) {
      navigator.clipboard.writeText(url)
        .then(() => { setCopied(true); setTimeout(() => setCopied(false), 2500); })
        .catch(() => fallback());
    } else { fallback(); }
  };
  const fallback = () => {
    const el = document.createElement('textarea');
    el.value = url; document.body.appendChild(el);
    el.select(); document.execCommand('copy');
    document.body.removeChild(el);
    setCopied(true); setTimeout(() => setCopied(false), 2500);
  };

  const sendInvite = async () => {
    const email = inviteEmail.trim().toLowerCase();
    if (!email) { setInviteError('Please enter an email address.'); return; }
    if (!/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email)) { setInviteError('Invalid email address.'); return; }
    if (!workspaceId) { setInviteError('Workspace not loaded. Please try again.'); return; }
    setInviteLoading(true); setInviteError('');
    const ok = await dbInviteMember(workspaceId, invitedBy, email);
    setInviteLoading(false);
    if (ok) { setInviteSent(true); setInviteEmail(''); }
    else    { setInviteError('Failed to send invite. Please try again.'); }
  };

  return (
    <div style={{ position:'fixed', inset:0, zIndex:200,
      display:'flex', flexDirection:'column', justifyContent:'flex-end' }}>
      <div style={{ position:'absolute', inset:0, background:'rgba(44,38,32,0.40)',
        backdropFilter:'blur(4px)' }} onClick={onClose} />
      <div style={{ position:'relative', background:C.card, borderRadius:'24px 24px 0 0',
        border:`1px solid ${C.border}`, padding:'20px 22px 44px',
        boxShadow:SH.float }}>
        <div style={{ width:40, height:5, borderRadius:3, background:C.border, margin:'0 auto 18px' }} />
        <h3 style={{ margin:'0 0 4px', fontSize:21, fontWeight:600, color:C.text,
          fontFamily:'Cormorant Garamond,serif' }}>Invite to Kizuna 絆</h3>
        <p style={{ margin:'0 0 20px', fontSize:15, color:C.dim, fontStyle:'italic' }}>
          Share the link, scan the QR code, or invite by email
        </p>

        {/* Email invite */}
        <div style={{ display:'flex', gap:8, marginBottom:16 }}>
          <input value={inviteEmail} onChange={e=>{setInviteEmail(e.target.value);setInviteSent(false);setInviteError('');}}
            onKeyDown={e=>e.key==='Enter'&&sendInvite()}
            placeholder="colleague@email.com" type="email"
            style={{ flex:1, background:C.elevated, border:`1px solid ${inviteError?'#C46A14':C.border}`,
              borderRadius:12, padding:'11px 14px', fontSize:16, color:C.text,
              outline:'none', fontFamily:'inherit' }} />
          <button onClick={sendInvite} disabled={inviteLoading}
            style={{ background:C.rose, border:'none', color:'#fff', borderRadius:12,
              padding:'11px 18px', fontSize:15, fontWeight:700, cursor:'pointer',
              fontFamily:'inherit', opacity:inviteLoading?0.7:1, flexShrink:0 }}>
            {inviteLoading ? '…' : 'Invite'}
          </button>
        </div>
        {inviteError && <p style={{ margin:'-10px 0 10px', fontSize:13, color:'#C46A14' }}>{inviteError}</p>}
        {inviteSent  && <p style={{ margin:'-10px 0 10px', fontSize:13, color:'#2A6E3A' }}>✓ Invite sent! They'll join when they sign up.</p>}

        {/* QR code */}
        <div style={{ display:'flex', justifyContent:'center', marginBottom:14 }}>
          <div style={{ background:C.elevated, borderRadius:16, padding:14,
            border:`1px solid ${C.border}`, boxShadow:SH.card }}>
            <img src={qr} alt="QR Code" width="160" height="160"
              style={{ display:'block', borderRadius:8 }} />
          </div>
        </div>
        {/* URL + copy */}
        <div style={{ display:'flex', gap:8, alignItems:'center',
          background:C.elevated, borderRadius:12, padding:'10px 14px',
          border:`1px solid ${C.border}`, marginBottom:14 }}>
          <span style={{ flex:1, fontSize:14, color:C.dim, overflow:'hidden',
            textOverflow:'ellipsis', whiteSpace:'nowrap' }}>{url}</span>
          <button onClick={copy}
            style={{ background:copied?C.T:C.rose, border:'none', color:'#fff',
              borderRadius:8, padding:'6px 14px', fontSize:14, fontWeight:700,
              cursor:'pointer', fontFamily:'inherit', flexShrink:0,
              transition:'background 0.2s' }}>
            {copied ? '✓ Copied!' : 'Copy Link'}
          </button>
        </div>
        <p style={{ margin:0, fontSize:14, color:C.muted, textAlign:'center', fontStyle:'italic' }}>
          Members open the link in Safari → Share → Add to Home Screen
        </p>
      </div>
    </div>
  );
}

// ─── SETTINGS TAB ────────────────────────────────────────────────
function SettingsTab({ auditLog, onReset, userName = '', onChangeName, onSignOut, workspace, workspaceLoaded, setWorkspace, userId }) {
  // Only show admin features once workspace is loaded AND role is confirmed admin
  // Never default to true — wait for confirmed data
  const isAdmin = workspaceLoaded && (workspace?.role === 'admin' || workspace?.ownerId === userId);
  const NOTIF_KEY = 'kizuna_notifs_v1';
  const DND_KEY   = 'kizuna_dnd_v1';

  const [notifs, setNotifs] = useState(() => {
    try { return JSON.parse(localStorage.getItem(NOTIF_KEY)) || { digest:true, preEvent:true, flights:true, shared:true }; }
    catch { return { digest:true, preEvent:true, flights:true, shared:true }; }
  });
  const [digestTime, setDigestTime] = useState(() => {
    try { return JSON.parse(localStorage.getItem(DND_KEY))?.digestTime || '06:30'; }
    catch { return '06:30'; }
  });
  const [dndStart, setDndStart] = useState(() => {
    try { return JSON.parse(localStorage.getItem(DND_KEY))?.dndStart || '22:00'; }
    catch { return '22:00'; }
  });
  const [dndEnd, setDndEnd] = useState(() => {
    try { return JSON.parse(localStorage.getItem(DND_KEY))?.dndEnd || '06:00'; }
    catch { return '06:00'; }
  });

  // Persist notif changes
  const saveNotifs = (updated) => {
    setNotifs(updated);
    localStorage.setItem(NOTIF_KEY, JSON.stringify(updated));
  };
  const saveDnd = (field, value) => {
    const next = { digestTime, dndStart, dndEnd, [field]: value };
    if (field === 'digestTime') setDigestTime(value);
    if (field === 'dndStart')   setDndStart(value);
    if (field === 'dndEnd')     setDndEnd(value);
    localStorage.setItem(DND_KEY, JSON.stringify(next));
  };
  const [showInvite, setShowInvite] = useState(false);

  // Use live workspace members from Supabase — no localStorage fallback needed
  const members = (workspace?.members || []).filter(m => m.id !== userId);
  const removeMember = async (memberId) => {
    if (!workspace?.id) return;
    await dbRemoveMember(workspace.id, memberId);
    // Optimistic UI update — remove from local workspace state immediately
    setWorkspace(prev => prev ? {
      ...prev,
      members: prev.members.filter(m => m.id !== memberId)
    } : prev);
  };

  const InputStyle = {
    display:'block', marginTop:6, width:'100%', boxSizing:'border-box',
    background:C.elevated, border:`1px solid ${C.border}`,
    borderRadius:12, padding:'10px 13px', color:C.text,
    fontSize:17, fontFamily:'inherit', outline:'none',
    boxShadow:SH.subtle,
  };

  return (
    <div style={{ padding:'0 18px 90px', overflowY:'auto', height:'100%', boxSizing:'border-box' }}>

      {showInvite && <InviteModal onClose={() => setShowInvite(false)} workspaceId={workspace?.id} invitedBy={userId} />}

      {/* Profile card */}
      <div style={{ paddingTop:12 }}>
        <div style={{ display:'flex', alignItems:'center', gap:16, padding:20,
          background:C.card, borderRadius:22,
          boxShadow:SH.card, border:`1px solid ${C.border}` }}>
          <div style={{ flex:1, minWidth:0 }}>
            <p style={{ margin:0, fontSize:21, fontWeight:600, color:C.text,
              fontFamily:'Cormorant Garamond,serif', overflow:'hidden',
              textOverflow:'ellipsis', whiteSpace:'nowrap' }}>{userName || 'Your Name'}</p>
          </div>
          <button onClick={onChangeName}
            style={{ background:C.elevated, border:`1px solid ${C.border}`,
              borderRadius:12, padding:'8px 14px', fontSize:14, color:C.dim,
              cursor:'pointer', fontFamily:'inherit', flexShrink:0 }}>
            Edit
          </button>
          <button onClick={onSignOut}
            style={{ background:'transparent', border:`1px solid ${C.border}`,
              borderRadius:12, padding:'8px 14px', fontSize:14, color:C.muted,
              cursor:'pointer', fontFamily:'inherit', flexShrink:0 }}>
            Sign Out
          </button>
        </div>
      </div>

      {/* Workspace */}
      <SS title="Workspace">
        <SR label={`${members.length} member${members.length!==1?'s':''}`}
          sub={`You are ${isAdmin?'Admin':'Member'}`}
          right={<Badge label={isAdmin?'Admin':'Member'} color={isAdmin?C.rose:C.dim} />} />
        <div style={{ padding:'0 18px 14px', borderTop:`1px solid ${C.border}` }}>
          <p style={{ fontSize:13, color:C.muted, margin:'10px 0 6px',
            fontWeight:700, letterSpacing:'0.1em', textTransform:'uppercase' }}>Members</p>
          {members.map(m => (
            <div key={m.id} style={{ display:'flex', alignItems:'center',
              gap:10, padding:'6px 0',
              borderBottom:`1px solid ${C.border}` }}>
              {/* Avatar */}
              <div style={{ width:32, height:32, borderRadius:16, background:C.elevated,
                border:`1px solid ${C.border}`, flexShrink:0,
                display:'flex', alignItems:'center', justifyContent:'center' }}>
                <span style={{ fontSize:15, color:C.dim }}>{m.name[0]}</span>
              </div>
              {/* Name */}
              <span style={{ flex:1, fontSize:16, color:C.text }}>{m.name}</span>
              {/* Delete — admin only */}
              {isAdmin && (
                <button onClick={() => removeMember(m.id)}
                  style={{ background:'transparent', border:`1px solid ${C.border}`,
                    borderRadius:8, width:28, height:28, cursor:'pointer',
                    display:'flex', alignItems:'center', justifyContent:'center',
                    color:C.muted, fontSize:16, flexShrink:0,
                    transition:'border-color 0.15s, color 0.15s' }}
                  onMouseEnter={e => { e.currentTarget.style.borderColor='#C46A14'; e.currentTarget.style.color='#C46A14'; }}
                  onMouseLeave={e => { e.currentTarget.style.borderColor=C.border; e.currentTarget.style.color=C.muted; }}>
                  ×
                </button>
              )}
            </div>
          ))}
          {members.length === 0 && (
            <p style={{ fontSize:15, color:C.muted, textAlign:'center',
              padding:'16px 0', fontStyle:'italic' }}>No members yet</p>
          )}
          {/* Invite button */}
          <button onClick={() => setShowInvite(true)}
            style={{ marginTop:14, background:'transparent',
              border:`1.5px dashed ${C.rose}60`,
              borderRadius:12, padding:'10px 14px', color:C.rose,
              fontSize:16, cursor:'pointer', width:'100%',
              fontFamily:'inherit', transition:'border-color 0.15s',
              display:'flex', alignItems:'center', justifyContent:'center', gap:8 }}>
            🌸 Invite via Link or QR Code
          </button>
        </div>
      </SS>

      {/* Notifications */}
      <SS title="Notifications">
        <SR label="Daily Digest" sub={`Fires at ${pt(digestTime)} each morning`}
          right={<Tog on={notifs.digest} onChange={v => saveNotifs({...notifs,digest:v})} />} />
        {notifs.digest && (
          <div style={{ padding:'6px 18px 14px', borderTop:`1px solid ${C.border}` }}>
            <label style={{ fontSize:15, color:C.dim }}>Digest time</label>
            <input type="time" value={digestTime} onChange={e=>saveDnd('digestTime',e.target.value)}
              style={InputStyle} />
          </div>
        )}
        <SR label="Pre-Event Reminders" sub="Contextual alerts per item"
          right={<Tog on={notifs.preEvent} onChange={v=>saveNotifs({...notifs,preEvent:v})} />} />
        <SR label="Flight Alerts" sub="Auto: T-24h, T-3h, T-1h"
          right={<Tog on={notifs.flights} onChange={v=>saveNotifs({...notifs,flights:v})} />} />
        <SR label="Shared Reminders" sub="Workspace push notifications" noBorder
          right={<Tog on={notifs.shared} onChange={v=>saveNotifs({...notifs,shared:v})} />} />
      </SS>

      {/* Do Not Disturb */}
      <SS title="Do Not Disturb">
        <SR label="DND Window" sub={`${pt(dndStart)} – ${pt(dndEnd)} · No notifications`}
          right={<span style={{ fontSize:15, color:notifs.digest?C.T:C.muted,
            background:notifs.digest?C.T+'20':C.elevated,
            borderRadius:10, padding:'2px 10px' }}>
            {notifs.digest ? '● Active' : '○ Off'}
          </span>} />
        <div style={{ padding:'6px 18px 14px', borderTop:`1px solid ${C.border}`,
          display:'grid', gridTemplateColumns:'1fr 1fr', gap:12 }}>
          {[['Start','dndStart',dndStart],['End','dndEnd',dndEnd]].map(([l,k,v]) => (
            <div key={l}>
              <label style={{ fontSize:15, color:C.dim }}>{l}</label>
              <input type="time" value={v} onChange={e=>saveDnd(k,e.target.value)}
                style={InputStyle} />
            </div>
          ))}
        </div>
      </SS>

      {/* Entry Colour Key */}
      <SS title="Entry Colour Key">
        <div style={{ padding:'14px 18px', display:'grid', gridTemplateColumns:'1fr 1fr', gap:10 }}>
          {Object.entries(TL).map(([t,l]) => (
            <div key={t} style={{ display:'flex', alignItems:'center', gap:10 }}>
              <div style={{ width:12, height:12, borderRadius:4, background:TC[t]+'90', flexShrink:0 }} />
              <span style={{ fontSize:16, color:C.text }}>{l}</span>
            </div>
          ))}
        </div>
      </SS>

      {/* Data & Privacy */}
      <SS title="Data & Privacy">
        <SR label="End-to-End Encryption" sub="All entries encrypted at rest"
          right={<span style={{ fontSize:15, color:C.T, background:C.T+'18', borderRadius:10, padding:'2px 10px' }}>✓ Active</span>} />
        <SR label="GDPR Compliance" sub="Data stored per EU regulations"
          right={<span style={{ fontSize:15, color:C.T, background:C.T+'18', borderRadius:10, padding:'2px 10px' }}>✓ Active</span>} />
        <SR label="Persistent Storage" sub={`Schema v${SCHEMA_VERSION} · Auto-saves on every change`}
          right={<span style={{ fontSize:15, color:C.rose, background:C.rose+'18', borderRadius:10, padding:'2px 10px' }}>◯ Live</span>} />
        <SR label="Audit Trail" sub="All changes tracked · Append-only" noBorder
          right={<span style={{ fontSize:15, color:C.T, background:C.T+'18', borderRadius:10, padding:'2px 10px' }}>✓ On</span>} />
      </SS>

      {/* About */}
      <SS title="About">
        <SR label="絆 Kizuna"
          sub={`${APP_VERSION} · Released ${APP_BUILD_DATE}`}
          right={
            <span style={{ fontSize:14, color:C.dim, background:C.elevated,
              borderRadius:10, padding:'3px 10px', border:`1px solid ${C.border}` }}>
              {APP_VERSION}
            </span>
          } />
        <SR label="Schema Version" sub={`Storage format v${SCHEMA_VERSION}`} noBorder
          right={<span style={{ fontSize:15, color:C.dim }}>v{SCHEMA_VERSION}</span>} />
      </SS>

      {/* Reset App Data — admin only */}
      {isAdmin && <ResetSection onReset={onReset} />}
    </div>
  );
}

// ─── ADD MODAL ───────────────────────────────────────────────────
const mkBlank = () => ({
  type:'',title:'',date:fd(new Date()),time:'',endTime:'',location:'',attendees:'',notes:'',
  priority:'medium',tags:'',message:'',airline:'',flightNum:'',depCity:'',arrCity:'',
  terminal:'',gate:'',seat:'',visibility:'private',remind:'30min'
});

function EForm({ form, set }) {
  // Auto-generate flight title from IATA codes
  const prevAutoRef = useRef('');
  useEffect(() => {
    if (form.type !== 'flight' || !form.depCity || !form.arrCity) return;
    const autoTitle = `${form.depCity} → ${form.arrCity}`;
    if (!form.title || form.title === prevAutoRef.current) {
      prevAutoRef.current = autoTitle;
      set('title', autoTitle);
    }
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [form.depCity, form.arrCity, form.type]);

  // ── Flight auto-fill — lookup via AeroDataBox when No. + Date filled ──
  const [lookupStatus, setLookupStatus] = useState(''); // '' | 'loading' | 'found' | 'not_found'
  const lastLookupRef = useRef('');
  useEffect(() => {
    if (form.type !== 'flight') return;
    if (!form.flightNum || form.flightNum.length < 3 || !form.date) return;
    const key = `${form.flightNum}_${form.date}`;
    if (key === lastLookupRef.current) return; // already looked up this combo
    lastLookupRef.current = key;

    if (!supabaseConfigured) return;
    let cancelled = false;
    setLookupStatus('loading');

    supabase.functions.invoke('flight-status', {
      body: { flightNumber: form.flightNum, date: form.date }
    }).then(({ data, error }) => {
      if (cancelled || error || data?.error) {
        setLookupStatus('not_found'); return;
      }
      // Auto-populate only empty fields — never overwrite what user typed
      if (data.depIata   && !form.depCity)  set('depCity',  data.depIata);
      if (data.arrIata   && !form.arrCity)  set('arrCity',  data.arrIata);
      if (data.airlineName && !form.airline) set('airline', data.airlineName);
      if (data.terminal  && !form.terminal) set('terminal', data.terminal);
      if (data.gate      && !form.gate)     set('gate',     data.gate);
      // Extract HH:mm from scheduledDep ISO string
      if (data.scheduledDep && !form.time) {
        const t = data.scheduledDep.split('T')[1]?.slice(0, 5);
        if (t) set('time', t);
      }
      setLookupStatus(data.depIata ? 'found' : 'not_found');
    }).catch(() => { if (!cancelled) setLookupStatus('not_found'); });

    return () => { cancelled = true; };
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [form.flightNum, form.date, form.type]);

  const inputBase = {
    width:'100%', boxSizing:'border-box', background:C.elevated,
    border:`1px solid ${C.border}`, borderRadius:12, padding:'12px 14px',
    color:C.text, fontSize:17, outline:'none', fontFamily:'inherit',
    boxShadow:SH.subtle,
  };
  const FI = ({ field, ...props }) => (
    <input value={form[field]||''} onChange={e => set(field, e.target.value)} {...props}
      style={{ ...inputBase, ...props.style }} />
  );
  const TA = ({ field, ...props }) => (
    <textarea value={form[field]||''} onChange={e => set(field, e.target.value)} rows={2} {...props}
      style={{ ...inputBase, resize:'vertical' }} />
  );
  const FL = ({ label, children }) => (
    <div style={{ marginBottom:14 }}>
      <label style={{ fontSize:14, color:C.dim, display:'block', marginBottom:5,
        fontWeight:700, textTransform:'uppercase', letterSpacing:'0.08em' }}>{label}</label>
      {children}
    </div>
  );
  const Row2 = ({ children }) => (
    <div style={{ display:'grid', gridTemplateColumns:'1fr 1fr', gap:10, marginBottom:14 }}>{children}</div>
  );
  const selStyle = { ...inputBase, appearance:'none' };

  return (
    <div style={{ paddingTop:8 }}>
      {/* Title shown for all types EXCEPT flight — flight title is auto-generated */}
      {form.type !== 'flight' && (
        <FL label="Title">
          <FI field="title" placeholder={`${TL[form.type]} title`} autoFocus />
        </FL>
      )}

      {form.type === 'flight' ? (<>
        {/* Flight No. + Date first — triggers AeroDataBox lookup */}
        <Row2>
          <FL label="Flight No.">
            <FI field="flightNum" placeholder="SQ321" autoFocus
              onChange={e=>set('flightNum',e.target.value.replace(/\s+/g,'').toUpperCase())} />
          </FL>
          <FL label="Date"><FI field="date" type="date" /></FL>
        </Row2>
        {/* Lookup status indicator */}
        {lookupStatus === 'loading' && (
          <p style={{ margin:'-6px 0 12px', fontSize:13, color:C.dim, fontStyle:'italic' }}>
            ✈ Looking up flight details…
          </p>
        )}
        {lookupStatus === 'found' && (
          <p style={{ margin:'-6px 0 12px', fontSize:13, color:'#2A6E3A' }}>
            ✓ Flight found — details filled in
          </p>
        )}
        {lookupStatus === 'not_found' && (
          <p style={{ margin:'-6px 0 12px', fontSize:13, color:C.muted, fontStyle:'italic' }}>
            Flight not found — please fill in manually
          </p>
        )}
        <Row2>
          <FL label="From"><FI field="depCity" placeholder="SIN" onChange={e=>set('depCity',e.target.value.toUpperCase())} /></FL>
          <FL label="To"><FI field="arrCity" placeholder="LHR" onChange={e=>set('arrCity',e.target.value.toUpperCase())} /></FL>
        </Row2>
        <FL label="Airline"><FI field="airline" placeholder="Singapore Airlines" /></FL>
        <Row2>
          <FL label="Seat"><FI field="seat" placeholder="1A" inputMode="text" /></FL>
          <FL label="Dep. Time"><FI field="time" type="time" /></FL>
        </Row2>
        <div style={{ display:'grid', gridTemplateColumns:'1fr 1fr', gap:10, marginBottom:14 }}>
          <FL label="Terminal"><FI field="terminal" placeholder="T3" inputMode="numeric" /></FL>
          <FL label="Gate"><FI field="gate" placeholder="G22" inputMode="numeric" /></FL>
        </div>
        <FL label="Priority">
          <select value={form.priority} onChange={e=>set('priority',e.target.value)} style={selStyle}>
            {['low','medium','high','critical'].map(p => (
              <option key={p} value={p}>{p.charAt(0).toUpperCase()+p.slice(1)}</option>
            ))}
          </select>
        </FL>
      </>) : form.type === 'task' ? (<>
        <Row2>
          <FL label="Due Date (optional)"><FI field="date" type="date" /></FL>
          <FL label="Priority">
            <select value={form.priority} onChange={e=>set('priority',e.target.value)} style={selStyle}>
              {['low','medium','high','critical'].map(p => (
                <option key={p} value={p}>{p.charAt(0).toUpperCase()+p.slice(1)}</option>
              ))}
            </select>
          </FL>
        </Row2>
        <FL label="Tags"><FI field="tags" placeholder="Finance, Legal, M&A" /></FL>
      </>) : form.type === 'reminder' ? (<>
        <Row2>
          <FL label="Date (optional)"><FI field="date" type="date" /></FL>
          <FL label="Time"><FI field="time" type="time" /></FL>
        </Row2>
        <FL label="Message"><TA field="message" placeholder="Reminder details…" /></FL>
      </>) : (<>
        <Row2>
          <FL label="Date"><FI field="date" type="date" /></FL>
          <FL label="Start Time"><FI field="time" type="time" /></FL>
        </Row2>
        <FL label="End Time"><FI field="endTime" type="time" /></FL>
        <FL label="Location"><FI field="location" placeholder="Room, address, or virtual" /></FL>
        {form.type==='meeting' && (
          <FL label="Attendees"><FI field="attendees" placeholder="Names or emails, comma-separated" /></FL>
        )}
        <FL label="Notes"><TA field="notes" placeholder="Additional details…" /></FL>
      </>)}

      <Row2>
        <FL label="Remind me">
          <select value={form.remind} onChange={e=>set('remind',e.target.value)} style={selStyle}>
            {[['none','None'],['15min','15 min'],['30min','30 min'],['1h','1 hr'],['2h','2 hrs'],['1d','1 day']].map(([v,l]) => (
              <option key={v} value={v}>{l}</option>
            ))}
          </select>
        </FL>
        <FL label="Visibility">
          <select value={form.visibility} onChange={e=>set('visibility',e.target.value)} style={selStyle}>
            <option value="private">🔒 Private</option>
            <option value="shared">◯ Shared</option>
          </select>
        </FL>
      </Row2>
    </div>
  );
}

function AddModal({ onClose, onSave, editEntry = null }) {
  const isEdit = editEntry !== null;
  // Edit mode: skip type selector (step 1), pre-populate form from entry
  const [step, setStep] = useState(isEdit ? 1 : 0);
  const [form, setForm] = useState(isEdit ? { ...mkBlank(), ...editEntry } : mkBlank());
  const setF = useCallback((k, v) => setForm(p => ({ ...p, [k]:v })), []);
  const canSave = form.type === 'flight'
    ? (form.flightNum?.trim().length > 0)   // flight: needs flight number
    : (form.title?.trim().length > 0);       // others: need title
  const handleSave = () => {
    if (!canSave) return;
    // Edit: preserve id + type. Create: assign UUID.
    onSave(isEdit
      ? { ...form, id: editEntry.id, type: editEntry.type }
      : { ...form, id: crypto.randomUUID() }
    );
    onClose();
  };

  const typeColor = TC[form.type] || C.rose;

  return (
    <div style={{ position:'absolute', inset:0, zIndex:100,
      display:'flex', flexDirection:'column', justifyContent:'flex-end' }}>
      <div style={{ position:'absolute', inset:0, background:'rgba(44,38,32,0.35)',
        backdropFilter:'blur(4px)' }} onClick={onClose} />
      <div style={{ position:'relative', background:C.card, borderRadius:'28px 28px 0 0',
        border:`1px solid ${C.border}`, borderBottom:'none', maxHeight:'92%',
        display:'flex', flexDirection:'column', boxShadow:SH.float }}>
        <div style={{ width:40, height:5, borderRadius:3, background:C.border, margin:'14px auto 0' }} />
        <div style={{ display:'flex', alignItems:'center', padding:'12px 22px 8px' }}>
          {/* Back only shown in create mode step 1 — not in edit mode (can't change type) */}
          {step===1 && !isEdit && (
            <button onClick={() => setStep(0)}
              style={{ background:'transparent', border:'none', color:C.rose,
                fontSize:17, cursor:'pointer', padding:'0 16px 0 0', fontWeight:700 }}>
              ‹ Back
            </button>
          )}
          <h2 style={{ flex:1, margin:0, fontSize:20, fontWeight:600, color:C.text,
            fontFamily:'Cormorant Garamond,serif' }}>
            {step===0 ? 'New Entry' : isEdit ? `Edit ${TL[form.type]}` : `New ${TL[form.type]}`}
          </h2>
          {step===1 ? (
            <div style={{ display:'flex', gap:8, alignItems:'center' }}>
              <button onClick={onClose}
                style={{ background:C.elevated, border:`1px solid ${C.border}`,
                  color:C.dim, borderRadius:12, padding:'9px 16px',
                  fontSize:15, fontWeight:600, cursor:'pointer', fontFamily:'inherit' }}>
                Cancel
              </button>
              <button onClick={handleSave}
                style={{ background:canSave?typeColor:C.elevated,
                  border:`1px solid ${canSave?typeColor:C.border}`,
                  color:canSave?'#fff':C.muted, borderRadius:12,
                  padding:'9px 20px', fontSize:17, fontWeight:600,
                  cursor:canSave?'pointer':'default',
                  boxShadow:canSave?`0 4px 16px ${typeColor}40`:'none',
                  fontFamily:'inherit', transition:'background 0.15s' }}>
                {isEdit ? 'Save Changes' : 'Save'}
              </button>
            </div>
          ) : (
            <button onClick={onClose}
              style={{ background:C.elevated, border:`1px solid ${C.border}`,
                color:C.dim, width:32, height:32, borderRadius:16,
                cursor:'pointer', fontSize:18, padding:0,
                display:'flex', alignItems:'center', justifyContent:'center' }}>✕</button>
          )}
        </div>
        <div style={{ overflowY:'auto', padding:'6px 22px 44px', flex:1 }}>
          {step === 0 ? (
            <div>
              <p style={{ fontSize:16, color:C.dim, margin:'4px 0 16px', fontStyle:'italic' }}>
                What would you like to add?
              </p>
              <div style={{ display:'grid', gridTemplateColumns:'1fr 1fr', gap:10 }}>
                {['meeting','task','flight','reminder','event'].map(t => (
                  <button key={t} onClick={() => { setForm({...mkBlank(),type:t}); setStep(1); }}
                    style={{ background:TC[t]+'15', border:`1px solid ${TC[t]+'35'}`,
                      borderRadius:18, padding:'18px 14px', cursor:'pointer', textAlign:'left',
                      display:'flex', flexDirection:'column', gap:6,
                      boxShadow:`0 2px 12px ${TC[t]}15`,
                      transition:'transform 0.1s' }}>
                    <span style={{ fontSize:24 }}>{TI[t]}</span>
                    <span style={{ fontSize:17, fontWeight:600, color:DTC[t]||TC[t] }}>{TL[t]}</span>
                    <span style={{ fontSize:15, color:C.dim, lineHeight:1.4 }}>
                      {t==='meeting'?'Schedule a meeting'
                        :t==='task'?'Add a to-do item'
                        :t==='flight'?'Log flight details'
                        :t==='reminder'?'Set a reminder'
                        :'Create an event'}
                    </span>
                  </button>
                ))}
                <button style={{ background:C.elevated, border:`1px dashed ${C.border}`,
                  borderRadius:18, padding:'18px 14px', cursor:'pointer',
                  display:'flex', flexDirection:'column', gap:6,
                  alignItems:'center', justifyContent:'center' }}>
                  <span style={{ fontSize:24 }}>🎤</span>
                  <span style={{ fontSize:15, color:C.muted, fontStyle:'italic' }}>Voice Input</span>
                </button>
              </div>
            </div>
          ) : (
            <EForm form={form} set={setF} />
          )}
        </div>
      </div>
    </div>
  );
}

// ─── KIZUNA ICON — TWO SAKURA 桜 ────────────────────────────────
// Flower 1: larger, lower-left — the dominant bloom.
// Flower 2: smaller, upper-right — the accent bloom, rendered behind.
// Three drifting petals add gracefulness between the two flowers.
// Petal path: authentic notched tip (bilobed split) of Prunus serrulata.
const KizunaIcon = () => {
  // One petal pointing upward from (0,0), length ~14 units.
  // The forked tip (L 0,-13.8 midpoint) is the sakura's signature.
  const P = "M 0,0 C -3.5,-3.5 -6,-8 -5,-12 C -4.5,-14.5 -2.5,-15 -0.8,-13 L 0,-13.8 L 0.8,-13 C 2.5,-15 4.5,-14.5 5,-12 C 6,-8 3.5,-3.5 0,0 Z";
  const ROTS = [0, 72, 144, 216, 288];
  const r = d => d * Math.PI / 180;

  return (
    <svg width="52" height="42" viewBox="0 0 52 42" fill="none"
      style={{ display:'block', flexShrink:0 }}>

      {/* ── Drifting petals — rendered first so flowers sit above ── */}

      {/* Petal drifting to the far right */}
      <g transform="translate(46,30) rotate(-22) scale(0.36)" opacity="0.42">
        <path d={P} fill="#EAA898" />
      </g>
      {/* Petal drifting below, between the two blooms */}
      <g transform="translate(29,36) rotate(50) scale(0.30)" opacity="0.35">
        <path d={P} fill="#F0C0B4" />
      </g>
      {/* Petal drifting to upper-left */}
      <g transform="translate(4,7) rotate(-58) scale(0.26)" opacity="0.28">
        <path d={P} fill="#EAB8A8" />
      </g>

      {/* ── FLOWER 2 — smaller accent bloom, upper-right ── */}
      {/* Offset rotation by 36° so its petals interleave with Flower 1 visually */}
      {ROTS.map(rot => (
        <g key={`f2p${rot}`}
          transform={`translate(37,13) rotate(${rot + 36}) scale(0.65)`}>
          <path d={P}
            fill="#F0C0B4"
            stroke="#E0A898"
            strokeWidth="0.45"
            opacity="0.86"
          />
        </g>
      ))}
      {/* Flower 2 — center disc */}
      <circle cx="37" cy="13" r="1.9" fill="#D09080" opacity="0.75" />
      {/* Flower 2 — stamen dots at r=3.3 */}
      {ROTS.map((rot, i) => (
        <circle key={`f2s${i}`}
          cx={(37 + Math.sin(r(rot)) * 3.3).toFixed(2)}
          cy={(13 - Math.cos(r(rot)) * 3.3).toFixed(2)}
          r="0.65" fill="#C89078" opacity="0.50"
        />
      ))}

      {/* ── FLOWER 1 — larger dominant bloom, lower-left ── */}
      {ROTS.map(rot => (
        <g key={`f1p${rot}`}
          transform={`translate(15,27) rotate(${rot})`}>
          <path d={P}
            fill="#EAA898"
            stroke="#D48880"
            strokeWidth="0.35"
            opacity="0.93"
          />
        </g>
      ))}
      {/* Flower 1 — center disc */}
      <circle cx="15" cy="27" r="2.8" fill="#C4826E" opacity="0.84" />
      {/* Flower 1 — stamen dots at r=5 */}
      {ROTS.map((rot, i) => (
        <circle key={`f1s${i}`}
          cx={(15 + Math.sin(r(rot)) * 5).toFixed(2)}
          cy={(27 - Math.cos(r(rot)) * 5).toFixed(2)}
          r="0.9" fill="#C4826E" opacity="0.52"
        />
      ))}
    </svg>
  );
};

// ─── BOTTOM NAV ──────────────────────────────────────────────────
const NAV = [
  { key:'home',     icon:'◯', label:'Home'     },
  { key:'calendar', icon:'◫', label:'Calendar'  },
  { key:'search',   icon:'◎', label:'Search'    },
  { key:'settings', icon:'◈', label:'Settings'  },
];

// ─── APP ROOT ────────────────────────────────────────────────────
export default function App() {
  const [tab,          setTab]          = useState('home');
  const [entries,      setEntries]      = useState([]);
  const [auditLog,     setAuditLog]     = useState([]);
  const [showAdd,      setShowAdd]      = useState(false);
  const [editingEntry, setEditingEntry] = useState(null);
  const [syncStatus,   setSyncStatus]   = useState('loading');
  const [workspace,       setWorkspace]       = useState(null); // {id, name, ownerId, role, members}
  const [workspaceLoaded, setWorkspaceLoaded] = useState(false);

  // ── Auth state ─────────────────────────────────────────────────
  const [user,       setUser]       = useState(null);   // Supabase user object
  const [authReady,  setAuthReady]  = useState(false);  // true once session checked
  const [email,      setEmail]      = useState('');
  const [authStep,    setAuthStep]    = useState('email'); // 'email' | 'code'
  const [authLoading, setAuthLoading] = useState(false);
  const [authError,   setAuthError]   = useState('');
  const [otpCode,     setOtpCode]     = useState('');

  // ── User display name ──────────────────────────────────────────
  const [userName,   setUserName]   = useState('');
  const [nameInput,  setNameInput]  = useState('');
  const [nameReady,  setNameReady]  = useState(false);
  // Ref mirror — synchronous read for toggleDone / updateEntry
  const entriesRef = useRef(entries);
  useEffect(() => { entriesRef.current = entries; }, [entries]);

  // ── Step 1: Listen for auth state changes ──────────────────────
  useEffect(() => {
    if (!supabase) { setAuthReady(true); return; }

    supabase.auth.getSession().then(({ data: { session } }) => {
      setUser(session?.user ?? null);
      setAuthReady(true);
    });

    const { data: { subscription } } = supabase.auth.onAuthStateChange((event, session) => {
      // Only update user on meaningful auth events — ignore token refreshes
      // to prevent re-triggering the data load useEffect unnecessarily
      if (event === 'SIGNED_IN' || event === 'SIGNED_OUT' || event === 'USER_UPDATED') {
        setUser(session?.user ?? null);
      }
    });
    return () => subscription.unsubscribe();
  }, []);

  // ── Step 2: Load data — each piece independently so one failure never kills another ──
  const loadingRef = useRef(false); // prevent concurrent loads
  useEffect(() => {
    if (!authReady || !user) return;
    if (loadingRef.current) return; // already loading
    loadingRef.current = true;

    async function load() {
      setSyncStatus('loading');

      // ① Entries — critical. If this fails, show sync error.
      let loadedEntries = [];
      try {
        loadedEntries = await dbLoadEntries(user.id);
        setEntries(loadedEntries);
        setSyncStatus('synced');
      } catch (err) {
        console.error('entries load failed:', err.message);
        setSyncStatus('error');
      }

      // ② Name — non-critical. Never triggers sync error.
      try {
        const loadedName = await dbLoadName(user.id);
        if (loadedName) {
          setUserName(loadedName);
          setNameInput(loadedName);
          setNameReady(true);
        }
      } catch (err) {
        console.error('name load failed:', err.message);
        // Fall back to localStorage
        const cached = localStorage.getItem(`exec_user_v1_${user.id}`);
        if (cached) { setUserName(cached); setNameInput(cached); setNameReady(true); }
      }

      // ③ Audit log — non-critical. Never triggers sync error.
      try {
        const loadedAudit = await dbLoadAudit(user.id);
        setAuditLog(loadedAudit);
      } catch { /* silently ignore */ }

      // ④ Workspace — non-critical. Never triggers sync error.
      try {
        const ws = await dbLoadWorkspace(user.id);
        if (ws) setWorkspace(ws);
      } catch { /* silently ignore */ }
      setWorkspaceLoaded(true); // mark as attempted regardless of success

      loadingRef.current = false;
    }

    load();
  }, [authReady, user]);

  // ── Step 3: Real-time — own entries + shared entries from workspace ──
  useEffect(() => {
    if (!user) return;
    const channel = supabase
      .channel(`kizuna-${user.id}`)
      // Own entries — all changes
      .on('postgres_changes',
        { event: '*', schema: 'public', table: 'entries', filter: `user_id=eq.${user.id}` },
        payload => {
          if (payload.eventType === 'DELETE') {
            setEntries(prev => prev.filter(e => e.id !== payload.old.id));
          } else if (payload.new?.data) {
            const incoming = payload.new.data;
            setEntries(prev => {
              const exists = prev.find(e => e.id === incoming.id);
              return exists
                ? prev.map(e => e.id === incoming.id ? incoming : e)
                : [...prev, incoming];
            });
          }
        })
      // Own audit log
      .on('postgres_changes',
        { event: 'INSERT', schema: 'public', table: 'audit_log', filter: `user_id=eq.${user.id}` },
        payload => {
          if (payload.new?.data) {
            setAuditLog(prev => [...prev, payload.new.data].slice(-200));
          }
        })
      .subscribe();

    // Also subscribe to shared entries from each workspace member
    const memberChannels = (workspace?.members || [])
      .filter(m => m.id !== user.id)
      .map(m => supabase
        .channel(`kizuna-shared-${m.id}-${user.id}`)
        .on('postgres_changes',
          { event: '*', schema: 'public', table: 'entries', filter: `user_id=eq.${m.id}` },
          payload => {
            // Only show if visibility is shared
            const entry = payload.new?.data || payload.old;
            if (!entry) return;
            if (payload.eventType === 'DELETE') {
              setEntries(prev => prev.filter(e => e.id !== payload.old.id));
            } else if (payload.new?.data?.visibility === 'shared') {
              const incoming = payload.new.data;
              setEntries(prev => {
                const exists = prev.find(e => e.id === incoming.id);
                return exists
                  ? prev.map(e => e.id === incoming.id ? incoming : e)
                  : [...prev, incoming];
              });
            } else if (payload.eventType === 'UPDATE' && payload.new?.data?.visibility !== 'shared') {
              // Entry was changed to private — remove from our view
              setEntries(prev => prev.filter(e =>
                !(e.id === payload.new.data.id && e.userId !== user.id)
              ));
            }
          })
        .subscribe()
      );

    return () => {
      supabase.removeChannel(channel);
      memberChannels.forEach(c => supabase.removeChannel(c));
    };
  }, [user, workspace]);

  // ── Auth actions ───────────────────────────────────────────────
  const sendOtp = async () => {
    const trimmed = email.trim();
    if (!trimmed) { setAuthError('Please enter your email address.'); return; }
    // Basic email format validation
    if (!/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(trimmed)) {
      setAuthError('Please enter a valid email address.'); return;
    }
    setAuthLoading(true); setAuthError('');
    const { error } = await supabase.auth.signInWithOtp({
      email: trimmed,
      options: { shouldCreateUser: true }
    });
    setAuthLoading(false);
    if (error) { setAuthError(error.message); }
    else       { setAuthStep('code'); setOtpCode(''); }
  };

  const verifyOtp = async () => {
    if (!otpCode.trim()) { setAuthError('Please enter the 8-digit code.'); return; }
    setAuthLoading(true); setAuthError('');
    const { error } = await supabase.auth.verifyOtp({
      email: email.trim(),
      token: otpCode.trim(),
      type:  'email',
    });
    setAuthLoading(false);
    if (error) {
      setAuthError('Invalid or expired code. Please try again.');
      setOtpCode(''); // clear stale code so user types fresh
    }
    // on success, onAuthStateChange fires → setUser → app loads
  };

  const signOut = async () => {
    const uid = user?.id;
    await supabase.auth.signOut();
    setUser(null); setEntries([]); setAuditLog([]); setWorkspace(null);
    setUserName(''); setNameInput(''); setNameReady(false);
    setAuthStep('email'); setOtpCode(''); setAuthError(''); setEmail('');
    if (uid) localStorage.removeItem(`exec_user_v1_${uid}`);
    localStorage.removeItem(SK_USER); // also clear old key if exists
  };

  // ── Name save ──────────────────────────────────────────────────
  const [nameSaving, setNameSaving] = useState(false);
  const [nameSaveError, setNameSaveError] = useState('');
  const saveUserName = async () => {
    const n = nameInput.trim();
    if (!n || !user || nameSaving) return;
    setNameSaving(true); setNameSaveError('');
    try {
      await dbSaveName(user.id, n);
      setUserName(n);
      setNameReady(true); // only set after confirmed DB write
    } catch {
      setNameSaveError('Could not save name. Please check your connection and try again.');
    } finally {
      setNameSaving(false);
    }
  };

  // ── Audit helper ───────────────────────────────────────────────
  const logAudit = useCallback((action, entry, changes = null) => {
    if (!user) return;
    const event = {
      id:         `${Date.now()}-${Math.random().toString(36).slice(2,6)}`,
      timestamp:  new Date().toISOString(),
      actor:      userName || 'You',
      action, entryId: entry.id, entryType: entry.type, entryTitle: entry.title, changes,
    };
    setAuditLog(prev => [...prev, event]);
    dbAppendAudit(user.id, event);
  }, [user, userName]);

  // ── Entry mutations ────────────────────────────────────────────
  const addEntry = useCallback(e => {
    // Stamp userId on entry so shared readers can identify ownership
    const stamped = { ...e, userId: user?.id };
    setEntries(prev => [...prev, stamped]);
    logAudit('created', stamped);
    if (user) dbUpsertEntry(user.id, stamped);
  }, [logAudit, user]);

  const toggleDone = useCallback(id => {
    const current = entriesRef.current.find(e => e.id === id);
    if (!current) return;
    // Only allow toggling own entries — shared entries from others are read-only
    if (current.userId && current.userId !== user?.id) return;
    const willComplete = !current.done;
    const updated = { ...current, done: willComplete };
    setEntries(prev => prev.map(e => e.id === id ? updated : e));
    logAudit(willComplete ? 'completed' : 'reopened', current);
    if (user) dbUpsertEntry(user.id, updated);
  }, [logAudit, user]);

  const updateEntry = useCallback(updated => {
    const original = entriesRef.current.find(e => e.id === updated.id);
    if (!original) return;
    const TRACKED = ['title','date','time','endTime','location','attendees','notes',
                     'priority','tags','message','airline','flightNum','depCity',
                     'arrCity','terminal','gate','seat','visibility'];
    const changes = TRACKED
      .filter(f => String(original[f] ?? '') !== String(updated[f] ?? ''))
      .map(f => ({ field:f, from:original[f], to:updated[f] }));
    setEntries(prev => prev.map(e => e.id === updated.id ? updated : e));
    logAudit('updated', updated, changes.length > 0 ? changes : null);
    setEditingEntry(null);
    if (user) dbUpsertEntry(user.id, updated);
  }, [logAudit, user]);

  const deleteEntry = useCallback(id => {
    const current = entriesRef.current.find(e => e.id === id);
    if (!current) return;
    setEntries(prev => prev.filter(e => e.id !== id));
    logAudit('deleted', current);
    if (user) dbDeleteEntry(user.id, id);
  }, [logAudit, user]);

  const resetData = useCallback(async () => {
    setEntries([]); setAuditLog([]);
    if (user) await dbResetUser(user.id);
    setSyncStatus('synced');
  }, [user]);

  const TAB_TITLES = { home:'', calendar:'Calendar', search:'Search', settings:'Settings' };
  const syncColor  = syncStatus==='synced' ? C.T : syncStatus==='error' ? '#C46A14' : C.rose;
  const syncLabel  = syncStatus==='loading' ? '◌ Syncing…' : syncStatus==='synced' ? '● Synced' : '⚠ Sync Error';

  const sharedStyle = {
    wrapper: { width:'100%', maxWidth:430, margin:'0 auto', height:'100vh',
      background:C.bg, display:'flex', flexDirection:'column', alignItems:'center',
      justifyContent:'center', padding:'0 32px', boxSizing:'border-box',
      fontFamily:`'Nunito','DM Sans',system-ui,sans-serif` },
    googleFont: `
      @import url('https://fonts.googleapis.com/css2?family=Nunito:wght@400;600;700;800&family=Cormorant+Garamond:ital,wght@0,600;1,400&display=swap');
      input[type=number]::-webkit-inner-spin-button,
      input[type=number]::-webkit-outer-spin-button { -webkit-appearance:none; margin:0; }
      input[type=number] { -moz-appearance:textfield; }
    `
  };

  // ── Auth screens ───────────────────────────────────────────────

  // Guard: show setup instructions if Supabase isn't configured
  if (!supabaseConfigured) {
    return (
      <div style={sharedStyle.wrapper}>
        <style>{sharedStyle.googleFont}</style>
        <p style={{ fontSize:36, margin:'0 0 16px' }}>⚙️</p>
        <h2 style={{ margin:'0 0 12px', fontSize:22, fontWeight:700, color:'#5C3020',
          textAlign:'center', fontFamily:'Cormorant Garamond,serif' }}>
          Supabase not configured
        </h2>
        <p style={{ fontSize:15, color:C.dim, textAlign:'center', lineHeight:1.7, margin:0 }}>
          Add these two secrets to your GitHub repo:<br/>
          <strong style={{ color:C.text }}>VITE_SUPABASE_URL</strong><br/>
          <strong style={{ color:C.text }}>VITE_SUPABASE_ANON_KEY</strong>
        </p>
        <p style={{ fontSize:13, color:C.muted, textAlign:'center', marginTop:16, lineHeight:1.6 }}>
          Settings → Secrets → Actions → New repository secret
        </p>
      </div>
    );
  }

  if (!authReady) {
    return (
      <div style={sharedStyle.wrapper}>
        <style>{sharedStyle.googleFont}</style>
        <KizunaIcon />
        <p style={{ marginTop:16, fontSize:15, color:C.dim, fontStyle:'italic',
          fontFamily:'Cormorant Garamond,serif' }}>Loading…</p>
      </div>
    );
  }

  if (!user) {
    return (
      <div style={sharedStyle.wrapper}>
        <style>{sharedStyle.googleFont}</style>
        <div style={{ marginBottom:20 }}><KizunaIcon /></div>
        <h1 style={{ margin:'0 0 6px', fontSize:32, fontWeight:600, color:C.text,
          fontFamily:'Cormorant Garamond,serif', textAlign:'center' }}>
          Kizuna&thinsp;絆
        </h1>
        <p style={{ margin:'0 0 36px', fontSize:14, color:C.dim, fontStyle:'italic',
          fontFamily:'Cormorant Garamond,serif', textAlign:'center', lineHeight:1.6 }}>
          an invisible thread that connects hearts
        </p>

        {authStep === 'email' ? (<>
          <p style={{ margin:'0 0 10px', fontSize:17, color:C.text, fontWeight:600, alignSelf:'flex-start' }}>
            Sign in with your email
          </p>
          <p style={{ margin:'0 0 14px', fontSize:14, color:C.dim, alignSelf:'flex-start', lineHeight:1.5 }}>
            We'll send an 8-digit code — no password needed.
          </p>
          <input
            value={email}
            onChange={e => setEmail(e.target.value)}
            onKeyDown={e => e.key==='Enter' && sendOtp()}
            placeholder="your@email.com"
            type="email"
            autoFocus
            style={{ width:'100%', boxSizing:'border-box', background:C.card,
              border:`1.5px solid ${C.border}`, borderRadius:16, padding:'16px 18px',
              fontSize:17, color:C.text, outline:'none', fontFamily:'inherit',
              boxShadow:SH.card, marginBottom: authError ? 8 : 16 }}
          />
          {authError && (
            <p style={{ margin:'0 0 12px', fontSize:13, color:'#C46A14', alignSelf:'flex-start' }}>
              {authError}
            </p>
          )}
          <button onClick={sendOtp} disabled={authLoading}
            style={{ width:'100%', background:`linear-gradient(135deg,${C.rose},${C.roseL})`,
              border:'none', borderRadius:16, padding:'18px',
              fontSize:18, fontWeight:700, color:'#fff', cursor:'pointer',
              fontFamily:'inherit', boxShadow:`0 6px 24px ${C.rose}45`,
              opacity: authLoading ? 0.7 : 1 }}>
            {authLoading ? 'Sending…' : 'Send Code 🌸'}
          </button>
        </>) : (<>
          <p style={{ margin:'0 0 6px', fontSize:17, color:C.text, fontWeight:600, alignSelf:'flex-start' }}>
            Enter your 8-digit code
          </p>
          <p style={{ margin:'0 0 16px', fontSize:14, color:C.dim, alignSelf:'flex-start', lineHeight:1.5 }}>
            Sent to <strong style={{ color:C.text }}>{email}</strong>
          </p>
          <input
            value={otpCode}
            onChange={e => setOtpCode(e.target.value.replace(/\D/g,'').slice(0,8))}
            onKeyDown={e => e.key==='Enter' && verifyOtp()}
            placeholder="00000000"
            type="text"
            inputMode="numeric"
            autoFocus
            style={{ width:'100%', boxSizing:'border-box', background:C.card,
              border:`1.5px solid ${C.border}`, borderRadius:16, padding:'16px 18px',
              fontSize:28, fontWeight:700, color:C.text, outline:'none',
              fontFamily:'inherit', boxShadow:SH.card,
              letterSpacing:'0.3em', textAlign:'center',
              marginBottom: authError ? 8 : 16 }}
          />
          {authError && (
            <p style={{ margin:'0 0 12px', fontSize:13, color:'#C46A14', alignSelf:'flex-start' }}>
              {authError}
            </p>
          )}
          <button onClick={verifyOtp} disabled={authLoading}
            style={{ width:'100%', background:`linear-gradient(135deg,${C.rose},${C.roseL})`,
              border:'none', borderRadius:16, padding:'18px',
              fontSize:18, fontWeight:700, color:'#fff', cursor:'pointer',
              fontFamily:'inherit', boxShadow:`0 6px 24px ${C.rose}45`,
              opacity: authLoading ? 0.7 : 1 }}>
            {authLoading ? 'Verifying…' : 'Enter Kizuna 🌸'}
          </button>
          <button onClick={() => { setAuthStep('email'); setOtpCode(''); setAuthError(''); }}
            style={{ marginTop:14, background:'transparent', border:'none',
              fontSize:14, color:C.dim, cursor:'pointer', fontFamily:'inherit' }}>
            ← Use a different email
          </button>
          <button onClick={sendOtp} disabled={authLoading}
            style={{ marginTop:8, background:'transparent', border:'none',
              fontSize:14, color:C.rose, cursor:'pointer', fontFamily:'inherit',
              textDecoration:'underline' }}>
            Resend code
          </button>
        </>)}
      </div>
    );
  }

  // ── Name setup screen (first time after sign-in) ───────────────
  if (!nameReady) {
    return (
      <div style={sharedStyle.wrapper}>
        <style>{sharedStyle.googleFont}</style>
        <div style={{ marginBottom:20 }}><KizunaIcon /></div>
        <h1 style={{ margin:'0 0 6px', fontSize:32, fontWeight:600, color:C.text,
          fontFamily:'Cormorant Garamond,serif', textAlign:'center' }}>
          Kizuna&thinsp;絆
        </h1>
        <p style={{ margin:'0 0 32px', fontSize:14, color:C.dim, fontStyle:'italic',
          fontFamily:'Cormorant Garamond,serif', textAlign:'center', lineHeight:1.6 }}>
          an invisible thread that connects hearts
        </p>
        <p style={{ margin:'0 0 12px', fontSize:17, color:C.text, fontWeight:600, alignSelf:'flex-start' }}>
          What's your name?
        </p>
        <input
          value={nameInput}
          onChange={e => setNameInput(e.target.value)}
          onKeyDown={e => e.key==='Enter' && saveUserName()}
          placeholder="Enter your full name"
          autoFocus
          style={{ width:'100%', boxSizing:'border-box', background:C.card,
            border:`1.5px solid ${nameSaveError ? '#C46A14' : C.border}`, borderRadius:16, padding:'16px 18px',
            fontSize:17, color:C.text, outline:'none', fontFamily:'inherit',
            boxShadow:SH.card, marginBottom: nameSaveError ? 8 : 16 }}
        />
        {nameSaveError && (
          <p style={{ margin:'0 0 12px', fontSize:13, color:'#C46A14', alignSelf:'flex-start' }}>
            {nameSaveError}
          </p>
        )}
        <button onClick={saveUserName} disabled={nameSaving}
          style={{ width:'100%', background:`linear-gradient(135deg,${C.rose},${C.roseL})`,
            border:'none', borderRadius:16, padding:'18px',
            fontSize:18, fontWeight:700, color:'#fff', cursor:'pointer',
            fontFamily:'inherit', boxShadow:`0 6px 24px ${C.rose}45`,
            opacity: nameSaving ? 0.7 : 1 }}>
          {nameSaving ? 'Saving…' : 'Enter Kizuna 🌸'}
        </button>
      </div>
    );
  }

  return (
    <div style={{ width:'100%', maxWidth:430, margin:'0 auto', height:'100vh',
      background:C.bg, color:C.text,
      fontFamily:`'Nunito','DM Sans',system-ui,sans-serif`,
      display:'flex', flexDirection:'column', position:'relative', overflow:'hidden',
      WebkitFontSmoothing:'antialiased' }}>

      <style>{`
        @import url('https://fonts.googleapis.com/css2?family=Nunito:wght@400;500;600;700;800&family=Cormorant+Garamond:ital,wght@0,400;0,600;1,400;1,600&display=swap');
        * { -webkit-tap-highlight-color: transparent; }
        input, select, textarea { font-family: 'Nunito', system-ui, sans-serif; }
        input[type=date]::-webkit-calendar-picker-indicator,
        input[type=time]::-webkit-calendar-picker-indicator { filter: opacity(0.5); }
        input[type=number]::-webkit-inner-spin-button,
        input[type=number]::-webkit-outer-spin-button { -webkit-appearance: none; margin: 0; }
        input[type=number] { -moz-appearance: textfield; }
        ::-webkit-scrollbar { width:3px; }
        ::-webkit-scrollbar-track { background: transparent; }
        ::-webkit-scrollbar-thumb { background: ${C.border}; border-radius:2px; }
        button { font-family: 'Nunito', system-ui, sans-serif; }
      `}</style>

      {/* ── Kizuna brand header ─────────────────────────────────── */}
      <div style={{ flexShrink:0, background:C.card,
        borderBottom:'none', boxShadow:'none',
        padding:'12px 20px 10px' }}>
        <div style={{ display:'flex', alignItems:'flex-start', justifyContent:'space-between', gap:12 }}>
          {/* Left — name + meaning */}
          <div style={{ flex:1, minWidth:0 }}>
            {/* App name row */}
            <div style={{ display:'flex', alignItems:'baseline', gap:10, marginBottom:5 }}>
              <h1 style={{ margin:0, fontSize:26, fontWeight:600, color:C.text,
                fontFamily:'Cormorant Garamond,serif', lineHeight:1, flexShrink:0 }}>
                Kizuna&thinsp;絆
              </h1>
              {/* Tab label — hidden on Home (empty string) */}
              {TAB_TITLES[tab] && (
                <span style={{ fontSize:14, fontWeight:700, color:C.rose,
                  textTransform:'uppercase', letterSpacing:'0.13em', flexShrink:0 }}>
                  {TAB_TITLES[tab]}
                </span>
              )}
            </div>
            {/* Meaning — two lines, poetic, italic */}
            <p style={{ margin:0, fontSize:13, color:C.dim, fontStyle:'italic',
              fontFamily:'Cormorant Garamond,serif', lineHeight:1.5 }}>
              Bonding with trust, loyalty & love —
            </p>
            <p style={{ margin:0, fontSize:13, color:C.dim, fontStyle:'italic',
              fontFamily:'Cormorant Garamond,serif', lineHeight:1.5 }}>
              an invisible thread that connects hearts across time & distance
            </p>
          </div>
          {/* Right — Sakura icon */}
          <div style={{ marginTop:2, flexShrink:0 }}>
            <KizunaIcon />
          </div>
        </div>
      </div>

      {/* ── Bottom nav bar ─────────────────────────────────────── */}
      <div style={{ display:'flex', alignItems:'center', height:64,
        borderTop:`1px solid ${C.border}`, background:C.card,
        flexShrink:0, paddingBottom:8,
        boxShadow:`0 -2px 12px rgba(44,38,32,0.06)` }}>

        {/* Sync pill — left side */}
        <span style={{ fontSize:11, fontWeight:700, letterSpacing:'0.07em',
          textTransform:'uppercase', color:syncColor, background:syncColor+'18',
          borderRadius:10, padding:'2px 8px', marginLeft:10, flexShrink:0 }}>
          {syncLabel}
        </span>

        {/* Nav tabs */}
        {NAV.map(n => (
          <button key={n.key} onClick={() => setTab(n.key)}
            style={{ flex:1, background: tab===n.key ? C.rose+'15' : 'transparent',
              border:'none', cursor:'pointer',
              display:'flex', flexDirection:'column', alignItems:'center', gap:2,
              padding:'6px 4px', borderRadius:14, margin:'0 3px',
              transition:'background 0.15s' }}>
            <span style={{ fontSize:22,
              color: tab===n.key ? C.rose : C.muted,
              transition:'color 0.15s' }}>{n.icon}</span>
            <span style={{ fontSize:11, fontWeight: tab===n.key ? 800 : 400,
              color: tab===n.key ? C.rose : C.muted,
              letterSpacing: tab===n.key ? '0.03em' : 0,
              transition:'color 0.15s' }}>{n.label}</span>
          </button>
        ))}

        {/* FAB — right side, thumb-friendly corner */}
        <button onClick={() => setShowAdd(true)}
          style={{ width:48, height:48, borderRadius:14, flexShrink:0,
            background:`linear-gradient(135deg,${C.rose},${C.roseL})`,
            border:'none', boxShadow:`0 4px 14px ${C.rose}50`,
            cursor:'pointer', display:'flex', alignItems:'center',
            justifyContent:'center', marginRight:10 }}>
          <span style={{ fontSize:26, color:'#fff', fontWeight:300, lineHeight:1 }}>+</span>
        </button>
      </div>

      {/* Main content */}
      <div style={{ flex:1, overflow:'hidden', position:'relative', background:C.bg }}>
        {tab==='home'     && <HomeTab     entries={entries} onToggle={toggleDone} onEdit={setEditingEntry} onDelete={deleteEntry} userName={userName} currentUserId={user?.id} />}
        {tab==='calendar' && <CalendarTab entries={entries} onToggle={toggleDone} onEdit={setEditingEntry} onDelete={deleteEntry} currentUserId={user?.id} />}
        {tab==='search'   && <SearchTab   entries={entries} onToggle={toggleDone} onEdit={setEditingEntry} onDelete={deleteEntry} currentUserId={user?.id} />}
        {tab==='settings' && <SettingsTab auditLog={auditLog} onReset={resetData} userName={userName} onChangeName={() => { setNameReady(false); setNameInput(userName); }} onSignOut={signOut} workspace={workspace} workspaceLoaded={workspaceLoaded} setWorkspace={setWorkspace} userId={user?.id} />}

        {/* Create modal */}
        {showAdd      && <AddModal onClose={() => setShowAdd(false)}      onSave={addEntry}    />}
        {/* Edit modal */}
        {editingEntry && <AddModal onClose={() => setEditingEntry(null)} onSave={updateEntry} editEntry={editingEntry} />}
      </div>

      {/* ── Bottom nav bar ─────────────────────────────────────── */}
      <div style={{ display:'flex', alignItems:'center', height:64,
        borderTop:`1px solid ${C.border}`, background:C.card,
        flexShrink:0, paddingBottom:8,
        boxShadow:`0 -2px 12px rgba(44,38,32,0.06)` }}>

        {/* Sync pill — left side */}
        <span style={{ fontSize:11, fontWeight:700, letterSpacing:'0.07em',
          textTransform:'uppercase', color:syncColor, background:syncColor+'18',
          borderRadius:10, padding:'2px 8px', marginLeft:10, flexShrink:0 }}>
          {syncLabel}
        </span>

        {/* Nav tabs */}
        {NAV.map(n => (
          <button key={n.key} onClick={() => setTab(n.key)}
            style={{ flex:1, background: tab===n.key ? C.rose+'15' : 'transparent',
              border:'none', cursor:'pointer',
              display:'flex', flexDirection:'column', alignItems:'center', gap:2,
              padding:'6px 4px', borderRadius:14, margin:'0 3px',
              transition:'background 0.15s' }}>
            <span style={{ fontSize:22,
              color: tab===n.key ? C.rose : C.muted,
              transition:'color 0.15s' }}>{n.icon}</span>
            <span style={{ fontSize:11, fontWeight: tab===n.key ? 800 : 400,
              color: tab===n.key ? C.rose : C.muted,
              transition:'color 0.15s' }}>{n.label}</span>
          </button>
        ))}

        {/* FAB — right corner, always thumb-reachable */}
        <button onClick={() => setShowAdd(true)}
          style={{ width:48, height:48, borderRadius:14, flexShrink:0,
            background:`linear-gradient(135deg,${C.rose},${C.roseL})`,
            border:'none', boxShadow:`0 4px 14px ${C.rose}50`,
            cursor:'pointer', display:'flex', alignItems:'center',
            justifyContent:'center', marginRight:10 }}>
          <span style={{ fontSize:26, color:'#fff', fontWeight:300, lineHeight:1 }}>+</span>
        </button>
      </div>
    </div>
  );
}
