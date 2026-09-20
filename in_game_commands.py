from __future__ import annotations

import asyncio, hashlib, logging, os, re, sqlite3
from datetime import UTC, datetime, timedelta
from typing import Any

import voting_api
from app import _call, _client, _dump, state
from match_leaderboard_api import app
from stats_tracker_api import _connect_db, _current_match_stats, _favorite_rows, _init_db, _player_row
from stats_commands_api import _match_revives

logger = logging.getLogger("hllv-rcon-bridge.chat-commands")
POLL_SECONDS=max(2.0,float(os.getenv("HLLV_CHAT_COMMAND_POLL_SECONDS","10")))
LOG_WINDOW_SECONDS=max(15,int(os.getenv("HLLV_CHAT_COMMAND_LOG_WINDOW_SECONDS","45")))
COOLDOWN_SECONDS=max(2,int(os.getenv("HLLV_CHAT_COMMAND_COOLDOWN_SECONDS","5")))
DISCORD=os.getenv("HLLV_DISCORD_URL","discord.gg/1stmi").strip()
WEBSITE=os.getenv("HLLV_WEBSITE_URL","1stmid.com").strip()
RULES=os.getenv("HLLV_RULES_TEXT","Use teamwork, follow server rules, and respect other players.").strip()
_task=None; _started=datetime.now(UTC); _cooldowns={}

PUBLIC={"!help","!commands","!discord","!website","!rules","!map","!nextmap","!time","!players","!score","!queue","!admins","!status","!rank","!kills","!deaths","!kd","!revives","!favorite","!top10","!votestatus"}
ADMIN={"!cancelvote","!startvote","!kick","!ban","!tempban","!unban","!warn","!message","!broadcast","!mapchange","!restartmatch","!addvip","!removevip","!history"}

def _now(): return datetime.now(UTC)
def _iso(v=None): return (v or _now()).isoformat().replace("+00:00","Z")
def _event_key(e): return hashlib.sha256(f"{getattr(e,'timestamp',None)}|{getattr(e,'raw_message',repr(e))}".encode()).hexdigest()
def _is_chat(e): return "playersendmessageadminlog" in type(e).__name__.lower()
def _pid(e): return str(getattr(e,"player_id","") or "").strip()
def _pname(e): return str(getattr(e,"player_name","") or _pid(e)).strip()
def _msg(e): return str(getattr(e,"message","") or "").strip()
def _entry_time(e):
    v=getattr(e,"timestamp",None)
    return (v if v.tzinfo else v.replace(tzinfo=UTC)) if isinstance(v,datetime) else None

def _init():
    _init_db()
    with _connect_db() as db:
        db.execute("CREATE TABLE IF NOT EXISTS general_chat_command_events(event_key TEXT PRIMARY KEY, seen_at TEXT NOT NULL)")

def _claim(e):
    try:
        with _connect_db() as db: db.execute("INSERT INTO general_chat_command_events VALUES(?,?)",(_event_key(e),_iso()))
        return True
    except sqlite3.IntegrityError: return False

async def _send(pid,text):
    await _call(_client().message_player(pid,str(text)[:500]))

def _walk_ids(obj):
    found=set()
    def walk(x):
        if isinstance(x,dict):
            for k,v in x.items():
                if str(k).lower() in {"id","player_id","playerid","eos_id","eosid"} and v: found.add(str(v))
                walk(v)
        elif isinstance(x,(list,tuple)): 
            for v in x: walk(v)
        elif hasattr(x,"model_dump"):
            try: walk(x.model_dump(by_alias=True))
            except Exception: pass
    walk(_dump(obj)); return found

async def _is_admin(pid):
    try: return pid in _walk_ids(await _call(_client().get_admin_users()))
    except Exception as exc:
        logger.warning("Admin permission lookup failed: %s",exc); return False

async def _players():
    return list(getattr(await _call(_client().get_players()),"players",[]) or [])

async def _find_player(token):
    token=token.strip().casefold(); ps=await _players()
    exact=[p for p in ps if str(getattr(p,"id","")).casefold()==token or str(getattr(p,"name","")).casefold()==token]
    if len(exact)==1:return exact[0]
    partial=[p for p in ps if token and token in str(getattr(p,"name","")).casefold()]
    if len(partial)==1:return partial[0]
    raise ValueError("PLAYER NOT FOUND OR NAME IS AMBIGUOUS")

def _duration(v):
    m=re.fullmatch(r"(\d+)([mhd]?)",v.lower())
    if not m: raise ValueError("DURATION MUST LOOK LIKE 30m, 2h OR 1d")
    n=int(m.group(1)); u=m.group(2) or "h"
    return max(1, (n+59)//60 if u=="m" else n*24 if u=="d" else n)

def _kd(k,d): return "INF" if d==0 and k>0 else "0.00" if d==0 else f"{k/d:.2f}"

async def _session():
    return _dump(await _call(_client().get_server_session()))

def _pick(d,*keys):
    for k in keys:
        if isinstance(d,dict) and d.get(k) not in (None,""): return d[k]
    return None

async def _public(entry,cmd,args):
    pid=_pid(entry); row=_player_row(pid) or {}; ps=None
    if cmd in {"!help","!commands"}:
        await _send(pid,"[ 1ST M.I. COMMANDS ]\nPUBLIC: !stats !matchstats !serverstats !leaderboard !topstats !topkills !toprevives !topkd !vote # !admin reason !reply msg\nMORE: !status !map !nextmap !time !players !rank !kd !revives !discord !website !rules")
    elif cmd=="!discord": await _send(pid,f"[ 1ST M.I. ]\nDISCORD: {DISCORD}")
    elif cmd=="!website": await _send(pid,f"[ 1ST M.I. ]\nWEBSITE: {WEBSITE}")
    elif cmd=="!rules": await _send(pid,f"[ 1ST M.I. SERVER RULES ]\n{RULES}")
    elif cmd in {"!map","!nextmap","!time","!players","!score","!queue","!status"}:
        s=await _session(); mp=_pick(s,"mapName","map_name","map","mapId","map_id") or "UNKNOWN"; nx=_pick(s,"nextMap","next_map","nextMapName","next_map_name") or "UNKNOWN"; tm=_pick(s,"remainingMatchTime","remaining_match_time","remainingTime","remaining_time") or "UNKNOWN"; pc=_pick(s,"playerCount","player_count","current_players") or len(await _players()); mx=_pick(s,"maxPlayerCount","max_player_count","max_players") or "?"; score=_pick(s,"score","teamScore","team_score") or "NOT EXPOSED"; queue=_pick(s,"queue","queueSize","queue_size") or "NOT EXPOSED"
        vals={"!map":f"CURRENT MAP: {mp}","!nextmap":f"NEXT MAP: {nx}","!time":f"TIME REMAINING: {tm}","!players":f"PLAYERS: {pc}/{mx}","!score":f"SCORE: {score}","!queue":f"QUEUE: {queue}","!status":f"MAP: {mp}\nNEXT: {nx}\nTIME: {tm}\nPLAYERS: {pc}/{mx}\nSCORE: {score}"}
        await _send(pid,"[ 1ST M.I. SERVER ]\n"+vals[cmd])
    elif cmd=="!admins":
        try: count=len(_walk_ids(await _call(_client().get_admin_users()))); await _send(pid,f"[ 1ST M.I. ]\nREGISTERED ADMINS: {count}")
        except Exception: await _send(pid,"[ 1ST M.I. ]\nADMIN STATUS UNAVAILABLE")
    elif cmd in {"!rank","!kills","!deaths","!kd","!revives","!favorite","!top10"}:
        with _connect_db() as db:
            rows=[dict(x) for x in db.execute("SELECT player_id,player_name,kills,deaths,revives FROM players ORDER BY kills DESC,deaths ASC").fetchall()]
        me=next((x for x in rows if str(x["player_id"])==pid),row)
        k=int(me.get("kills") or 0); d=int(me.get("deaths") or 0); rv=int(me.get("revives") or 0)
        if cmd=="!rank": text=f"SERVER KILL RANK: #{next((i for i,x in enumerate(rows,1) if str(x['player_id'])==pid),'?')} / {len(rows)}"
        elif cmd=="!kills": text=f"SERVER KILLS: {k}"
        elif cmd=="!deaths": text=f"SERVER DEATHS: {d}"
        elif cmd=="!kd": text=f"SERVER K/D: {_kd(k,d)}"
        elif cmd=="!revives": text=f"SERVER REVIVES: {rv}"
        elif cmd=="!favorite":
            w,v=_favorite_rows(pid); text=f"FAV WEAPON: {(w or {}).get('name','N/A')}\nFAV VEHICLE: {(v or {}).get('name','N/A')}"
        else: text="TOP 10 KILLS\n"+"\n".join(f"{i}. {x['player_name']} - {x['kills']}" for i,x in enumerate(rows[:10],1))
        await _send(pid,"[ 1ST M.I. STATS ]\n"+text)
    elif cmd=="!votestatus":
        v=voting_api._state.get("active")
        if not v: await _send(pid,"[ 1ST M.I. VOTE ]\nNO ACTIVE VOTE")
        else: await _send(pid,"[ 1ST M.I. VOTE ]\n"+voting_api._announcement(v,reminder=True))

async def _admin(entry,cmd,args):
    pid=_pid(entry); name=_pname(entry)
    if not await _is_admin(pid):
        await _send(pid,"[ 1ST M.I. ADMIN ]\nACCESS DENIED - ADMIN COMMAND"); return
    if cmd=="!broadcast":
        if not args: raise ValueError("USAGE: !broadcast <message>")
        await _call(_client().broadcast(args)); return
    if cmd=="!mapchange":
        if not args: raise ValueError("USAGE: !mapchange <map>")
        await _call(_client().change_map(args)); return
    if cmd=="!cancelvote":
        v=voting_api._state.get("active")
        if not v: raise ValueError("NO ACTIVE VOTE")
        await voting_api._finish_vote(v,announce=True,cancelled=True); return
    if cmd=="!startvote":
        raise ValueError("START VOTES FROM THE CONTROLLER - MULTI-ANSWER CONFIGURATION IS REQUIRED")
    if cmd=="!restartmatch":
        for method in ("restart_match","restart_game","restart_map"):
            fn=getattr(_client(),method,None)
            if callable(fn): await _call(fn()); return
        raise ValueError("CURRENT RCON LIBRARY DOES NOT EXPOSE MATCH RESTART")
    parts=args.split()
    if cmd in {"!kick","!ban","!unban","!warn","!message","!addvip","!removevip","!tempban","!history"} and not parts: raise ValueError(f"USAGE: {cmd} <player> ...")
    if cmd=="!history":
        p=await _find_player(parts[0]); target=str(getattr(p,"id","")); r=_player_row(target) or {}; await _send(pid,f"[ PLAYER HISTORY ]\n{getattr(p,'name',target)}\nK {r.get('kills',0)} D {r.get('deaths',0)} R {r.get('revives',0)}\nFIRST {r.get('first_seen','N/A')}\nLAST {r.get('last_seen','N/A')}"); return
    p=await _find_player(parts[0]); target=str(getattr(p,"id","")); tname=str(getattr(p,"name",target))
    if cmd=="!kick":
        reason=" ".join(parts[1:]) or "Removed by server administration"; await _call(_client().kick_player(target,reason))
    elif cmd=="!ban":
        reason=" ".join(parts[1:]) or "Banned by server administration"; await _call(_client().ban_player(target,reason,name))
    elif cmd=="!tempban":
        if len(parts)<2: raise ValueError("USAGE: !tempban <player> <30m|2h|1d> [reason]")
        hours=_duration(parts[1]); reason=" ".join(parts[2:]) or "Banned by server administration"; await _call(_client().ban_player(target,reason,name,duration_hours=hours))
    elif cmd=="!unban":
        ok=False
        for method in ("remove_temporary_ban","remove_permanent_ban"):
            try: await _call(getattr(_client(),method)(target)); ok=True
            except Exception: pass
        if not ok: raise ValueError("BAN NOT FOUND")
    elif cmd in {"!warn","!message"}:
        message=" ".join(parts[1:])
        if not message: raise ValueError(f"USAGE: {cmd} <player> <message>")
        await _send(target,("[ 1ST M.I. ADMIN WARNING ]\n" if cmd=="!warn" else "[ 1ST M.I. ADMIN MESSAGE ]\n")+message)
    elif cmd=="!addvip": await _call(_client().add_vip(target,f"Added in-game by {name}"))
    elif cmd=="!removevip": await _call(_client().remove_vip(target))
    await _send(pid,f"[ 1ST M.I. ADMIN ]\n{cmd[1:].upper()} COMPLETE: {tname}")

async def _process():
    entries=list(getattr(await _call(_client().get_admin_log(seconds_span=LOG_WINDOW_SECONDS)),"entries",[]) or [])
    entries.sort(key=lambda e:_entry_time(e) or datetime.min.replace(tzinfo=UTC))
    for e in entries:
        if not _is_chat(e): continue
        raw=_msg(e); cmd=(raw.split(maxsplit=1)[0].lower() if raw else ""); args=(raw.split(maxsplit=1)[1].strip() if len(raw.split(maxsplit=1))>1 else "")
        if cmd not in PUBLIC|ADMIN: continue
        et=_entry_time(e)
        if et and et < _started-timedelta(seconds=2): _claim(e); continue
        if not _claim(e): continue
        pid=_pid(e); key=(pid,cmd); prev=_cooldowns.get(key)
        if prev and (_now()-prev).total_seconds()<COOLDOWN_SECONDS: continue
        _cooldowns[key]=_now()
        try:
            if cmd in ADMIN: await _admin(e,cmd,args)
            else: await _public(e,cmd,args)
        except Exception as exc:
            logger.warning("Command %s failed for %s: %s",cmd,pid,exc)
            try: await _send(pid,f"[ 1ST M.I. COMMAND ]\n{str(exc)[:420]}")
            except Exception: pass

async def _worker():
    logger.info("General in-game commands started; admin commands require live RCON admin membership")
    while True:
        try:
            if state.client is not None and state.client.is_connected(): await _process()
        except asyncio.CancelledError: raise
        except Exception as exc: logger.warning("General command worker failed: %s",exc)
        await asyncio.sleep(POLL_SECONDS)

@app.on_event("startup")
async def start_general_commands():
    global _task
    _init(); _task=asyncio.create_task(_worker(),name="hllv-general-chat-commands")

@app.on_event("shutdown")
async def stop_general_commands():
    global _task
    if _task:
        _task.cancel()
        try: await _task
        except asyncio.CancelledError: pass
        _task=None

@app.get("/api/v2/chat-commands/status")
async def chat_commands_status():
    return {"ok":True,"public":sorted(PUBLIC),"admin":sorted(ADMIN),"permission_source":"HLLV RCON admin list","poll_seconds":POLL_SECONDS}
