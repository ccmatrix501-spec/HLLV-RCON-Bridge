from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from fastapi import HTTPException, Request

from admin_logs_api import _serialize_entry, app
from app import _call, _client, _ok

logger = logging.getLogger("hllv-rcon-bridge.voting")

STATE_FILE = Path(os.getenv("VOTE_STATE_FILE", "/data/hllv-voting.json"))
POLL_SECONDS = max(1.0, float(os.getenv("VOTE_POLL_SECONDS", "2")))
LOG_WINDOW_SECONDS = max(10, int(os.getenv("VOTE_LOG_WINDOW_SECONDS", "30")))
MAX_HISTORY = 50
MAX_TEMPLATES = 50
MAX_ANSWERS = 6

_state: dict[str, Any] = {"active": None, "history": [], "templates": []}
_worker_task: asyncio.Task | None = None
_state_lock = asyncio.Lock()


def _now() -> datetime:
    return datetime.now(UTC)


def _iso(value: datetime | None = None) -> str:
    return (value or _now()).isoformat().replace("+00:00", "Z")


def _parse_iso(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00")).astimezone(UTC)
    except Exception:
        return None


def _load_state() -> None:
    global _state
    try:
        if not STATE_FILE.exists():
            return
        raw = json.loads(STATE_FILE.read_text(encoding="utf-8"))
        if not isinstance(raw, dict):
            return
        _state = {
            "active": raw.get("active"),
            "history": list(raw.get("history") or [])[-MAX_HISTORY:],
            "templates": list(raw.get("templates") or [])[:MAX_TEMPLATES],
        }
    except Exception as exc:
        logger.warning("Could not load vote state: %s", exc)


def _save_state() -> None:
    try:
        STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
        tmp = STATE_FILE.with_suffix(STATE_FILE.suffix + ".tmp")
        tmp.write_text(json.dumps(_state, indent=2, ensure_ascii=False), encoding="utf-8")
        tmp.replace(STATE_FILE)
    except Exception as exc:
        logger.warning("Could not persist vote state: %s", exc)


def _counts(vote: dict[str, Any]) -> list[int]:
    counts = [0 for _ in vote.get("answers", [])]
    for ballot in (vote.get("votes") or {}).values():
        try:
            choice = int(ballot.get("choice"))
        except Exception:
            continue
        if 0 <= choice < len(counts):
            counts[choice] += 1
    return counts


def _result(vote: dict[str, Any]) -> dict[str, Any]:
    counts = _counts(vote)
    if not counts or sum(counts) == 0:
        return {"winner_indexes": [], "winner_answers": [], "winning_votes": 0, "tie": False}
    high = max(counts)
    winners = [i for i, count in enumerate(counts) if count == high]
    answers = vote.get("answers") or []
    return {
        "winner_indexes": winners,
        "winner_answers": [answers[i] for i in winners],
        "winning_votes": high,
        "tie": len(winners) > 1,
    }


def _public_vote(vote: dict[str, Any] | None) -> dict[str, Any] | None:
    if not vote:
        return None
    counts = _counts(vote)
    ends_at = _parse_iso(vote.get("ends_at"))
    remaining = max(0, int((ends_at - _now()).total_seconds())) if ends_at else 0
    return {
        "id": vote.get("id"),
        "status": vote.get("status"),
        "vote_type": vote.get("vote_type"),
        "question": vote.get("question"),
        "answers": vote.get("answers") or [],
        "counts": counts,
        "total_votes": sum(counts),
        "allow_changes": bool(vote.get("allow_changes", True)),
        "announce_result": bool(vote.get("announce_result", True)),
        "confirm_votes": bool(vote.get("confirm_votes", True)),
        "reminder_interval_seconds": int(vote.get("reminder_interval_seconds") or 0),
        "duration_seconds": int(vote.get("duration_seconds") or 0),
        "started_at": vote.get("started_at"),
        "ends_at": vote.get("ends_at"),
        "ended_at": vote.get("ended_at"),
        "remaining_seconds": remaining,
        "last_error": vote.get("last_error"),
        "result": vote.get("result") or (_result(vote) if vote.get("status") != "active" else None),
    }


def _vote_type_label(value: str) -> str:
    labels = {
        "custom": "CUSTOM VOTE",
        "map": "MAP VOTE",
        "game_mode": "GAME MODE VOTE",
        "yes_no": "YES / NO VOTE",
        "event": "EVENT VOTE",
        "restart": "RESTART VOTE",
    }
    return labels.get(value, value.replace("_", " ").upper())


def _duration_label(seconds: int) -> str:
    if seconds % 3600 == 0:
        n = seconds // 3600
        return f"{n} HOUR" + ("S" if n != 1 else "")
    if seconds % 60 == 0:
        n = seconds // 60
        return f"{n} MINUTE" + ("S" if n != 1 else "")
    return f"{seconds} SECONDS"


def _announcement(vote: dict[str, Any], reminder: bool = False) -> str:
    answers = vote.get("answers") or []
    lines = [
        "[ 1ST M.I. SERVER VOTE ]",
        "",
        _vote_type_label(str(vote.get("vote_type") or "custom")),
        "",
        str(vote.get("question") or "SERVER VOTE"),
        "",
    ]
    for index, answer in enumerate(answers, start=1):
        lines.append(f"{index} - {answer}")
    ends_at = _parse_iso(vote.get("ends_at"))
    remaining = max(0, int((ends_at - _now()).total_seconds())) if ends_at else int(vote.get("duration_seconds") or 0)
    lines += [
        "",
        "TYPE !vote <number> IN TEAM OR UNIT CHAT",
        f"VOTING CLOSES IN {_duration_label(remaining)}" if reminder else f"VOTING OPEN FOR {_duration_label(int(vote.get('duration_seconds') or 0))}",
    ]
    return "\n".join(lines)


def _result_announcement(vote: dict[str, Any]) -> str:
    result = vote.get("result") or _result(vote)
    if not result.get("winner_answers"):
        winner = "NO VALID VOTES RECEIVED"
    elif result.get("tie"):
        winner = "TIE: " + " / ".join(result["winner_answers"])
    else:
        winner = str(result["winner_answers"][0]).upper()
    return "\n".join([
        "[ 1ST M.I. VOTE RESULT ]",
        "",
        _vote_type_label(str(vote.get("vote_type") or "custom")),
        "",
        str(vote.get("question") or "SERVER VOTE"),
        "",
        "RESULT:",
        winner,
        "",
        f"TOP VOTES: {int(result.get('winning_votes') or 0)}",
        f"TOTAL VOTES: {sum(_counts(vote))}",
        "",
        "THANK YOU FOR VOTING.",
    ])


async def _message_everyone(message: str) -> dict[str, int]:
    client = _client()
    players_response = await _call(client.get_players())
    players = list(getattr(players_response, "players", []) or [])
    sent = 0
    failed = 0
    for player in players:
        player_id = str(getattr(player, "id", "") or "").strip()
        if not player_id:
            failed += 1
            continue
        try:
            await _call(client.message_player(player_id, message))
            sent += 1
        except Exception as exc:
            failed += 1
            logger.debug("Vote message failed for %s: %s", player_id, exc)
    return {"sent": sent, "failed": failed, "online": len(players)}


async def _confirm_vote(player_id: str, answer: str, changed: bool) -> None:
    text = "VOTE UPDATED" if changed else "VOTE RECORDED"
    message = f"[ 1ST M.I. SERVER VOTE ]\n\n{text}\nYOUR CHOICE: {answer}"
    try:
        await _call(_client().message_player(player_id, message))
    except Exception as exc:
        logger.debug("Vote confirmation failed for %s: %s", player_id, exc)


def _resolve_choice(vote: dict[str, Any], token: str) -> int | None:
    token = token.strip()
    if token.isdigit():
        index = int(token) - 1
        return index if 0 <= index < len(vote.get("answers") or []) else None
    lowered = token.casefold()
    exact = [i for i, answer in enumerate(vote.get("answers") or []) if str(answer).casefold() == lowered]
    return exact[0] if len(exact) == 1 else None


async def _poll_chat(vote: dict[str, Any]) -> None:
    try:
        response = await _call(_client().get_admin_log(seconds_span=LOG_WINDOW_SECONDS))
    except Exception as exc:
        vote["last_error"] = f"Chat log poll failed: {exc}"
        _save_state()
        return

    started_at = _parse_iso(vote.get("started_at")) or _now()
    entries = list(getattr(response, "entries", []) or [])
    serialized = [_serialize_entry(entry) for entry in entries]
    serialized.sort(key=lambda item: str(item.get("timestamp") or ""))

    changed_state = False
    confirmations: list[tuple[str, str, bool]] = []
    for entry in serialized:
        if entry.get("type") != "CHAT":
            continue
        timestamp = _parse_iso(str(entry.get("timestamp") or ""))
        if timestamp and timestamp < started_at:
            continue
        message = str(entry.get("message") or "").strip()
        match = re.match(r"^!vote\s+(.+?)\s*$", message, flags=re.IGNORECASE)
        if not match:
            continue
        player_id = str(entry.get("player_id") or "").strip()
        if not player_id:
            continue
        choice = _resolve_choice(vote, match.group(1))
        if choice is None:
            continue

        ballots = vote.setdefault("votes", {})
        existing = ballots.get(player_id)
        if existing and not vote.get("allow_changes", True):
            continue
        if existing and int(existing.get("choice", -1)) == choice:
            continue

        answer = str(vote["answers"][choice])
        ballots[player_id] = {
            "choice": choice,
            "player_name": str(entry.get("player_name") or player_id),
            "updated_at": str(entry.get("timestamp") or _iso()),
        }
        changed_state = True
        if vote.get("confirm_votes", True):
            confirmations.append((player_id, answer, bool(existing)))

    if changed_state:
        vote["last_error"] = None
        _save_state()
    for player_id, answer, changed in confirmations:
        await _confirm_vote(player_id, answer, changed)


async def _finish_vote(vote: dict[str, Any], *, announce: bool = True, cancelled: bool = False) -> None:
    async with _state_lock:
        current = _state.get("active")
        if not current or current.get("id") != vote.get("id"):
            return
        vote["status"] = "cancelled" if cancelled else "complete"
        vote["ended_at"] = _iso()
        vote["result"] = _result(vote)
        snapshot = json.loads(json.dumps(vote))
        _state["history"] = (list(_state.get("history") or []) + [snapshot])[-MAX_HISTORY:]
        _state["active"] = None
        _save_state()

    if cancelled:
        if announce:
            try:
                await _message_everyone("[ 1ST M.I. SERVER VOTE ]\n\nTHE CURRENT VOTE HAS BEEN CANCELLED BY AN ADMIN.")
            except Exception:
                pass
        return

    if announce and vote.get("announce_result", True):
        try:
            await _message_everyone(_result_announcement(vote))
        except Exception as exc:
            logger.warning("Could not announce vote result: %s", exc)


async def _vote_worker(vote_id: str) -> None:
    logger.info("Vote worker started for %s", vote_id)
    while True:
        vote = _state.get("active")
        if not vote or vote.get("id") != vote_id or vote.get("status") != "active":
            return
        ends_at = _parse_iso(vote.get("ends_at"))
        if not ends_at or _now() >= ends_at:
            await _finish_vote(vote, announce=True)
            return

        await _poll_chat(vote)

        reminder_interval = int(vote.get("reminder_interval_seconds") or 0)
        next_reminder = _parse_iso(vote.get("next_reminder_at"))
        if reminder_interval > 0 and next_reminder and _now() >= next_reminder:
            try:
                await _message_everyone(_announcement(vote, reminder=True))
                vote["next_reminder_at"] = _iso(_now() + timedelta(seconds=reminder_interval))
                _save_state()
            except Exception as exc:
                vote["last_error"] = f"Reminder failed: {exc}"
                vote["next_reminder_at"] = _iso(_now() + timedelta(seconds=reminder_interval))
                _save_state()

        await asyncio.sleep(POLL_SECONDS)


def _ensure_worker() -> None:
    global _worker_task
    vote = _state.get("active")
    if not vote or vote.get("status") != "active":
        return
    if _worker_task and not _worker_task.done():
        return
    _worker_task = asyncio.create_task(_vote_worker(str(vote.get("id"))))


_load_state()


@app.on_event("startup")
async def _resume_vote() -> None:
    vote = _state.get("active")
    if vote and vote.get("status") == "active":
        ends_at = _parse_iso(vote.get("ends_at"))
        if ends_at and ends_at > _now():
            _ensure_worker()
        else:
            await _finish_vote(vote, announce=False)


@app.get("/api/v2/votes")
async def get_votes() -> dict[str, Any]:
    return {
        "active": _public_vote(_state.get("active")),
        "history": [_public_vote(vote) for vote in reversed(list(_state.get("history") or [])[-10:])],
        "templates": list(_state.get("templates") or []),
        "command": "!vote <number>",
        "storage_file": str(STATE_FILE),
    }


@app.post("/api/v2/votes")
async def start_vote(request: Request) -> dict[str, Any]:
    body = await request.json()
    vote_type = str(body.get("vote_type") or "custom").strip().lower()
    question = str(body.get("question") or "").strip()
    answers = [str(item).strip() for item in list(body.get("answers") or []) if str(item).strip()]
    duration_seconds = int(body.get("duration_seconds") or 0)
    allow_changes = body.get("allow_changes") is not False
    announce_result = body.get("announce_result") is not False
    confirm_votes = body.get("confirm_votes") is not False
    reminder_interval_seconds = int(body.get("reminder_interval_seconds") or 0)

    if vote_type not in {"custom", "map", "game_mode", "yes_no", "event", "restart"}:
        raise HTTPException(status_code=400, detail="Invalid vote_type")
    if not question or len(question) > 140:
        raise HTTPException(status_code=400, detail="question is required and must be 140 characters or fewer")
    if not 2 <= len(answers) <= MAX_ANSWERS:
        raise HTTPException(status_code=400, detail=f"Provide between 2 and {MAX_ANSWERS} vote answers")
    if any(len(answer) > 60 for answer in answers):
        raise HTTPException(status_code=400, detail="Each answer must be 60 characters or fewer")
    if duration_seconds < 15 or duration_seconds > 86400:
        raise HTTPException(status_code=400, detail="Vote duration must be between 15 seconds and 24 hours")
    if reminder_interval_seconds and (reminder_interval_seconds < 15 or reminder_interval_seconds >= duration_seconds):
        raise HTTPException(status_code=400, detail="Reminder interval must be at least 15 seconds and shorter than the vote duration")

    async with _state_lock:
        if _state.get("active") and _state["active"].get("status") == "active":
            raise HTTPException(status_code=409, detail="Another server vote is already active")
        now = _now()
        vote = {
            "id": str(uuid.uuid4()),
            "status": "active",
            "vote_type": vote_type,
            "question": question,
            "answers": answers,
            "duration_seconds": duration_seconds,
            "allow_changes": allow_changes,
            "announce_result": announce_result,
            "confirm_votes": confirm_votes,
            "reminder_interval_seconds": reminder_interval_seconds,
            "started_at": _iso(now),
            "ends_at": _iso(now + timedelta(seconds=duration_seconds)),
            "next_reminder_at": _iso(now + timedelta(seconds=reminder_interval_seconds)) if reminder_interval_seconds else None,
            "ended_at": None,
            "votes": {},
            "result": None,
            "last_error": None,
        }
        _state["active"] = vote
        _save_state()

    try:
        delivery = await _message_everyone(_announcement(vote))
    except Exception as exc:
        vote["last_error"] = f"Initial announcement failed: {exc}"
        _save_state()
        delivery = {"sent": 0, "failed": 0, "online": 0}

    _ensure_worker()
    return _ok(vote=_public_vote(vote), delivery=delivery)


@app.post("/api/v2/votes/end")
async def end_vote() -> dict[str, Any]:
    vote = _state.get("active")
    if not vote:
        raise HTTPException(status_code=404, detail="No active vote")
    public_before = _public_vote(vote)
    await _finish_vote(vote, announce=True)
    return _ok(vote=public_before, result=vote.get("result"))


@app.post("/api/v2/votes/cancel")
async def cancel_vote() -> dict[str, Any]:
    vote = _state.get("active")
    if not vote:
        raise HTTPException(status_code=404, detail="No active vote")
    await _finish_vote(vote, announce=True, cancelled=True)
    return _ok(cancelled=True)


@app.post("/api/v2/votes/templates")
async def save_vote_template(request: Request) -> dict[str, Any]:
    body = await request.json()
    name = str(body.get("name") or "").strip()
    config = dict(body.get("config") or {})
    if not name or len(name) > 60:
        raise HTTPException(status_code=400, detail="Template name is required and must be 60 characters or fewer")
    template = {
        "id": str(uuid.uuid4()),
        "name": name,
        "config": config,
        "created_at": _iso(),
    }
    templates = [item for item in list(_state.get("templates") or []) if str(item.get("name", "")).casefold() != name.casefold()]
    templates.append(template)
    _state["templates"] = templates[-MAX_TEMPLATES:]
    _save_state()
    return _ok(template=template)


@app.delete("/api/v2/votes/templates/{template_id}")
async def delete_vote_template(template_id: str) -> dict[str, Any]:
    before = len(_state.get("templates") or [])
    _state["templates"] = [item for item in list(_state.get("templates") or []) if item.get("id") != template_id]
    if len(_state["templates"]) == before:
        raise HTTPException(status_code=404, detail="Vote template not found")
    _save_state()
    return _ok(deleted=template_id)
