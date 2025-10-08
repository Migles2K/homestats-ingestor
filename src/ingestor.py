import os, json, time, argparse, math, urllib.parse, sys
import requests
import gspread
from google.oauth2.service_account import Credentials

TIMEZONE = "America/Sao_Paulo"
HEADER = [
  "Team","Season","League","Date","Opponent","Half",
  "Goals_For","Goals_Against",
  "xG_For","xG_Against",
  "Possession_For%","Possession_Against%",
  "BigChances_For","BigChances_Against",
  "Shots_For","Shots_Against",
  "ShotsOT_For","ShotsOT_Against",
  "Corners_For","Corners_Against",
  "Saves_For","Saves_Against",
  "YellowCards_For","YellowCards_Against",
  "RedCards_For","RedCards_Against",
  "BTTS","Kickoff","Stadium","GoalEvents_For","GoalEvents_Against"
]

TOURNAMENTS = {
  "La Liga":            {"tid":8,   "sid":77559},
  "Premier League":     {"tid":17,  "sid":76986},
  "Bundesliga":         {"tid":35,  "sid":77333},
  "Champions League":   {"tid":7,   "sid":76953},
  "Serie A":            {"tid":23,  "sid":76457},
  "Brasileirão":        {"tid":325, "sid":72034},
  "Ligue 1":            {"tid":34,  "sid":77356},
  "Eredivisie":         {"tid":37,  "sid":77012},
  "Liga Portugal":      {"tid":238, "sid":77806},
  "Liga Profesional":   {"tid":18817,"sid":71738},
  "Süper Lig":          {"tid":52,  "sid":77805},
  "MLS":                {"tid":242, "sid":70158},
  "Europa League":      {"tid":679, "sid":76984},
  "Sul-Americana":      {"tid":480, "sid":70070},
}

def season_for(league):
  l = (league or "").lower()
  south = any(k in l for k in ["brasileir", "libertadores", "sudamericana", "sul-americana", "liga profesional"])
  return "2025" if south else "2025/26"

def dt_fmt(ts, fmt):
  return time.strftime(fmt, time.localtime(ts))

def http_get(url, tries=4, backoff=0.7):
  base = os.getenv("PROXY_BASE_URL", "").strip()
  headers = {"User-Agent":"Mozilla/5.0 (compatible; Ingestor/1.0)"}
  for i in range(tries):
    try:
      if base:
        u = base.rstrip("/") + "/fetch?url=" + urllib.parse.quote(url, safe="")
        r = requests.get(u, timeout=25, headers=headers)
      else:
        r = requests.get(url, timeout=25, headers=headers)
      if r.status_code==200:
        return r.json()
      if r.status_code in (403,429,503):
        time.sleep(backoff*(i+1))
        continue
      r.raise_for_status()
    except Exception:
      if i==tries-1: return None
      time.sleep(backoff*(i+1))
  return None

def list_rounds(tid,sid):
  r = http_get(f"https://api.sofascore.com/api/v1/unique-tournament/{tid}/season/{sid}/rounds")
  arr=[]
  if r and (r.get("rounds") or r.get("data")):
    src = r.get("rounds") or r.get("data")
    for x in src:
      v = x.get("number") or x.get("round") or x.get("id")
      if isinstance(v,int): arr.append(v)
  if arr: return sorted(set(arr))
  found=[]; miss=0
  for n in range(1,61):
    ev = http_get(f"https://api.sofascore.com/api/v1/unique-tournament/{tid}/season/{sid}/events/round/{n}")
    lst = (ev or {}).get("events") or (ev or {}).get("data") or []
    if lst:
      found.append(n); miss=0
    else:
      miss+=1
      if miss>=6 and found: break
    time.sleep(0.06)
  return found

def events_for_round(tid,sid,rn):
  a = http_get(f"https://api.sofascore.com/api/v1/unique-tournament/{tid}/season/{sid}/events/round/{rn}")
  if a and ((a.get("events") or a.get("data"))):
    return a.get("events") or a.get("data") or []
  b = http_get(f"https://api.sofascore.com/api/v1/unique-tournament/{tid}/season/{sid}/events?round={rn}")
  if b and ((b.get("events") or b.get("data"))):
    return b.get("events") or b.get("data") or []
  return []

def per_half(stats_root, incidents):
  out = {"first":{"home":{},"away":{}}, "second":{"home":{},"away":{}}}
  AL = {
    "poss": ["ball possession","possession"],
    "shots":["total shots","shots total","shots"],
    "sot":["shots on target","on target shots","shots on target (inc. blocked)","shots on target (inc blocked)"],
    "corners":["corner kicks","corners"],
    "saves":["saves","goalkeeper saves"],
    "big":["big chances","big chances created"],
    "xg":["expected goals (xg)","expected goals","xg"],
    "yel":["yellow cards","yellow card"],
    "red":["red cards","red card"]
  }
  def norm(s): return ("" if s is None else str(s)).lower().encode("ascii","ignore").decode()
  def is_first(s): s=norm(s); return "first" in s or "1st" in s or "first_half" in s
  def is_second(s): s=norm(s); return "second" in s or "2nd" in s or "second_half" in s
  def set_if(dst,name,val):
    k=norm(name); v2 = float(str(val).replace("%","")) if isinstance(val,str) and val.strip().endswith("%") else val
    for key,arr in AL.items():
      if any(norm(a)==k for a in arr): dst[key]=v2
  def feed_items(lst,dst):
    for it in lst or []:
      n = it.get("name") or it.get("title") or it.get("key") or ""
      set_if(dst["home"],n,it.get("home"))
      set_if(dst["away"],n,it.get("away"))
  def feed_block(block,dst):
    if block and isinstance(block.get("groups"),list):
      for g in block["groups"]: feed_items(g.get("statisticsItems"),dst)
    if block and isinstance(block.get("statisticsItems"),list):
      feed_items(block["statisticsItems"],dst)
    if block and isinstance(block.get("statistics"),list):
      for inner in block["statistics"]: feed_items(inner.get("statisticsItems"),dst)

  top = (stats_root or {}).get("statistics") or stats_root or []
  seq = top if isinstance(top,list) else [top]
  for entry in seq:
    tag = entry.get("period") or entry.get("name") or entry.get("groupName") or ""
    dest = "first" if is_first(tag) else ("second" if is_second(tag) else None)
    if dest: feed_block(entry,out[dest])
    elif isinstance(entry.get("groups"),list):
      for g in entry["groups"]:
        t = g.get("groupName") or g.get("name") or ""
        if is_first(t): feed_block(g,out["first"])
        elif is_second(t): feed_block(g,out["second"])

  count={"first":{"home":{"yel":0,"red":0},"away":{"yel":0,"red":0}},
         "second":{"home":{"yel":0,"red":0},"away":{"yel":0,"red":0}}}
  for i in incidents or []:
    side = "home" if i.get("isHome") else "away"
    minute = ((i.get("time") or {}).get("minute") or 0)
    half = "first" if minute<=45 else "second"
    t = (i.get("type") or i.get("incidentType") or "").lower()
    if "yellow" in t: count[half][side]["yel"]+=1
    elif "red" in t: count[half][side]["red"]+=1
    elif t=="card":
      color = ((i.get("card") or {}).get("color") or "").lower()
      if "yellow" in color: count[half][side]["yel"]+=1
      if "red" in color: count[half][side]["red"]+=1

  for h in ["first","second"]:
    for s in ["home","away"]:
      o=out[h][s]
      if o.get("xg") is None: o["xg"]="-"
      if o.get("big") is None: o["big"]="-"
      for k in ["shots","sot","corners","saves","poss","yel","red"]:
        if o.get(k) is None: o[k]="-"
      if o.get("yel")=="-": o["yel"]=count[h][s]["yel"]
      if o.get("red")=="-": o["red"]=count[h][s]["red"]

  for h in ["first","second"]:
    H=out[h]["home"]; A=out[h]["away"]
    if H.get("poss") not in ("-",None) and (A.get("poss") in ("-",None)): A["poss"]=100-float(H["poss"])
    if A.get("poss") not in ("-",None) and (H.get("poss") in ("-",None)): H["poss"]=100-float(A["poss"])
  return out

def goals_by_half(inc):
  out={"home":{"H1":0,"H2":0,"eventsH1":"","eventsH2":""},
       "away":{"H1":0,"H2":0,"eventsH1":"","eventsH2":""}}
  ev={"home":{"H1":[],"H2":[]},"away":{"H1":[],"H2":[]}}
  goals=[g for g in (inc or []) if (str(g.get("type") or g.get("incidentType") or "").lower()=="goal" and not g.get("cancelled") and not g.get("isCancelled"))]
  for g in goals:
    side="home" if g.get("isHome") else "away"
    minute=((g.get("time") or {}).get("minute") or g.get("minute") or 0)
    add=((g.get("time") or {}).get("addedTime") or (g.get("time") or {}).get("injuryTime") or g.get("addedTime"))
    disp=(f"{minute}+{add}" if add else str(minute))
    player=((g.get("player") or {}).get("name") or (g.get("player") or {}).get("shortName") or
            (g.get("scorer") or {}).get("name") or (g.get("scorer") or {}).get("shortName") or
            g.get("playerName") or g.get("scorerName") or "")
    isP = bool(g.get("isPenalty") or (str(g.get("shotType") or "").lower()=="penalty"))
    isOG= bool(g.get("isOwnGoal"))
    half = "H1" if minute<=45 else "H2"
    ev[side][half].append(f"{disp}' {player}{' (p)' if isP else ''}{' (og)' if isOG else ''}")
  for s in ["home","away"]:
    for h in ["H1","H2"]:
      ev[s][h].sort(key=lambda x: (int(x.split("'")[0].split("+")[0]), int(x.split("+")[1].split("'")[0]) if "+" in x else 0))
      out[s][h]=len(ev[s][h]); out[s]["eventsH1" if h=="H1" else "eventsH2"]="; ".join(ev[s][h])
  return out

def row_half(half, team, opp, season, league, date, hs, side, kickoff, stadium, btts, goal_info):
  opp_side = "away" if side=="home" else "home"
  S = hs[half].get(side,{})
  O = hs[half].get(opp_side,{})
  gf = goal_info[side]["H1" if half=="first" else "H2"]
  ga = goal_info[opp_side]["H1" if half=="first" else "H2"]
  evFor = goal_info[side]["eventsH1" if half=="first" else "eventsH2"]
  evAg  = goal_info[opp_side]["eventsH1" if half=="first" else "eventsH2"]
  def INT(v): 
    if v in ("-",None,""): return "-"
    try: return int(float(v))
    except: return "-"
  def PCT(v):
    if v in ("-",None,""): return "-"
    return str(v).replace("%","")
  def XGF(v):
    if v in ("-",None,""): return "-"
    try: return f"{round(float(v),2):.2f}"
    except: return "-"
  yFor = 0 if S.get("yel") in ("-",None) else INT(S.get("yel"))
  yAg  = 0 if O.get("yel") in ("-",None) else INT(O.get("yel"))
  rFor = 0 if S.get("red") in ("-",None) else INT(S.get("red"))
  rAg  = 0 if O.get("red") in ("-",None) else INT(O.get("red"))
  return [
    team,season,league,date,opp, "first" if half=="first" else "second",
    gf,ga,
    XGF(S.get("xg")),XGF(O.get("xg")),
    PCT(S.get("poss")),PCT(O.get("poss")),
    S.get("big","-"),O.get("big","-"),
    S.get("shots","-"),O.get("shots","-"),
    S.get("sot","-"),O.get("sot","-"),
    S.get("corners","-"),O.get("corners","-"),
    S.get("saves","-"),O.get("saves","-"),
    yFor,yAg,rFor,rAg,
    btts,kickoff,stadium,evFor,evAg
  ]

def build_rows(event_id, league):
  ev = http_get(f"https://api.sofascore.com/api/v1/event/{event_id}") or {}
  if not ev.get("event"): return None
  e = ev["event"]
  home = (e.get("homeTeam") or {}).get("name") or ""
  away = (e.get("awayTeam") or {}).get("name") or ""
  stats = http_get(f"https://api.sofascore.com/api/v1/event/{event_id}/statistics") or {}
  inc   = (http_get(f"https://api.sofascore.com/api/v1/event/{event_id}/incidents") or {}).get("incidents") or []
  leagueName = league
  season = season_for(leagueName)
  ts = e.get("startTimestamp") or 0
  date = dt_fmt(ts, "%d/%m/%Y")
  kickoff = dt_fmt(ts, "%H:%M")
  stadium = ((e.get("venue") or {}).get("name") or "")
  hs = (e.get("homeScore") or {}).get("current")
  as_ = (e.get("awayScore") or {}).get("current")
  btts = "yes" if (isinstance(hs,int) and isinstance(as_,int) and hs>0 and as_>0) else "no"
  goals = goals_by_half(inc)
  ph = per_half(stats, inc)
  r1 = row_half("first", home, away, season, leagueName, date, ph, "home", kickoff, stadium, btts, goals)
  r2 = row_half("second", home, away, season, leagueName, date, ph, "home", kickoff, stadium, btts, goals)
  return [r1, r2]

def open_sheet():
  sa_json = os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"]
  info = json.loads(sa_json)
  creds = Credentials.from_service_account_info(info, scopes=["https://www.googleapis.com/auth/spreadsheets"])
  gc = gspread.authorize(creds)
  sh = gc.open_by_key(os.environ["SPREADSHEET_ID"])
  return sh

def get_or_create_tab(sh, name, reset=False):
  try:
    ws = sh.worksheet(name)
    if reset:
      sh.del_worksheet(ws)
      ws = sh.add_worksheet(title=name, rows=2000, cols=len(HEADER))
  except gspread.exceptions.WorksheetNotFound:
    ws = sh.add_worksheet(title=name, rows=2000, cols=len(HEADER))
  if ws.row_count < 1: ws.add_rows(1)
  if ws.col_count < len(HEADER): ws.add_cols(len(HEADER)-ws.col_count)
  current_header = ws.row_values(1)
  if current_header != HEADER:
    ws.resize(rows=1, cols=len(HEADER))
    ws.update("A1", [HEADER])
  return ws

def get_index_ws(sh):
  try:
    ws = sh.worksheet("_index")
  except gspread.exceptions.WorksheetNotFound:
    ws = sh.add_worksheet(title="_index", rows=2, cols=4)
    ws.update("A1", [["League","EventId","Round","RowStart"]])
    ws.hide()
  return ws

def load_index_set(ws, league):
  values = ws.get_all_values()
  s=set()
  for r in values[1:]:
    if len(r)>=2 and r[0]==league:
      try: s.add(int(r[1]))
      except: pass
  return s

def append_index(ws, league, event_id, round_no, row_start):
  ws.append_row([league, int(event_id), int(round_no), int(row_start)], value_input_option="RAW")

def main():
  parser = argparse.ArgumentParser()
  parser.add_argument("--league", required=True)
  parser.add_argument("--mode", required=True, choices=["from-scratch","update"])
  args = parser.parse_args()

  lg = args.league
  if lg not in TOURNAMENTS:
    print("Unknown league", lg); sys.exit(1)
  tid = TOURNAMENTS[lg]["tid"]; sid = TOURNAMENTS[lg]["sid"]

  sh = open_sheet()
  ws = get_or_create_tab(sh, lg, reset=(args.mode=="from-scratch"))
  if args.mode=="from-scratch":
    ws.update("A1", [HEADER])

  idx_ws = get_index_ws(sh)
  idx_set = load_index_set(idx_ws, lg) if args.mode=="update" else set()

  rounds = list_rounds(tid, sid)
  for rn in rounds:
    events = events_for_round(tid, sid, rn)
    events = [e for e in events if str((e.get("status") or {}).get("type","")).lower() in ("finished","ft","after overtime","after penalties")]
    events.sort(key=lambda e: e.get("startTimestamp",0))
    title = f"Round {rn}"
    col_a = [a[0] for a in (ws.get_all_values() or [])]
    if title not in col_a:
      ws.append_row([title]+[""]*(len(HEADER)-1), value_input_option="RAW")
    for e in events:
      eid = int(e.get("id"))
      if args.mode=="update" and eid in idx_set:
        continue
      rows = build_rows(eid, lg)
      if not rows: continue
      start_row = ws.row_count+1
      ws.append_rows(rows, value_input_option="RAW")
      append_index(idx_ws, lg, eid, rn, start_row)
      time.sleep(0.15)

if __name__ == "__main__":
  main()
