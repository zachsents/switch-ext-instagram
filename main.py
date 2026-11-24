#!/usr/bin/env python3
"""instagram-dms: The user's Instagram DMs, read without opening a chat.

Replies to what the user (or I, from his account) asked by DM, like a booking or an
inquiry, land in his Instagram inbox, which has no API for a personal account.
This sense reads it in this Mac's own Chrome, signed in as the user, through the
macOS appendage's browser: one background tab (session `instagram-dms`) on a
bare instagram.com page that replays the inbox page's own GraphQL queries
(page.js). Nothing opens a chat, so nothing is marked read on his phone and
nobody sees "Seen". The queries are captured from the inbox page itself when
they're missing or stop working, and stay in Instagram's own page storage.

It records each conversation as `instagram-thread:<thread id>` in
`comms/instagram` whenever its latest message, read state or receipts change:
who, the latest message and who sent it, whether the user has read it, whether they
have seen his, what's unanswered, and the last few messages in full. An
observation whose `new_from_them` is true is the first sight of a new message
from the other side (`new_messages` lists them); a wake on that is what acts.

Every 2 minutes while a conversation is live (The user wrote and waits on them, or
they wrote in the last half hour), else every 5.

Its keeper hosts it as an MCP server (sbmcp) (actions `check`, `thread`, `send`).
"""

import datetime
import json
import os
import random
import re
import subprocess
import sys
import threading
import time

import sbmcp

app = sbmcp.Server('instagram-dms', version='5', reaches={'calls': ['browser_act']}, connections=[{'provider': 'instagram', 'kind': 'session', 'label': 'Instagram', 'scopes': [], 'multiple': False}], instructions="The user's Instagram DMs (the account signed in to Chrome on this Mac), read without opening a chat, so nothing shows Seen: replays the inbox page's own queries from a background tab every 2 minutes while a conversation is live, else every 5. Records each conversation as instagram-thread:<thread id> in comms/instagram whenever its latest message, read state or receipts change: who, the latest message and who sent it, unread, whether they've seen the user's, what's unanswered, the last few messages in full and a link; new_from_them (with new_messages) marks the first sight of a new message from the other side. Actions: check reads the inbox now; thread reads one conversation's history without opening it; send sends a DM in the user's name.")
app.event('thread', "A DM conversation: a new message, a read, who's waiting on whom", stream='comms/instagram', kind='observation')
app.event('heartbeat', 'How the sense stands: polling, signed out, errors', stream='vitals/instagram-dms', kind='observation', retention='signal')


INSTANCE = os.environ.get("SB_INSTANCE", "instagram-dms@local")
HOST = INSTANCE.split("@", 1)[1]
BROWSER = f"macos@{HOST}"
SESSION = "instagram-dms"
STREAM = "comms/instagram"
VITALS = "vitals/instagram-dms"
SENSE = "sense:instagram-dms"
BASE = "https://www.instagram.com"
PARK = BASE + "/robots.txt"  # same origin, nothing on it runs or opens a chat
LIVE_S, NORMAL_S = 120, 300
LIVE_WAIT_H = 6
SIGNED_OUT_S = 600
SIGNED_OUT_AGAIN_H = 6
CANCEL_PAUSE_S = 15 * 60
HEARTBEAT_S = 600
KEEP = 6  # messages kept per conversation
PAGE_JS = open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "page.js")).read()

poll_lock = threading.Lock()
wake = threading.Event()
convs = {}  # entity -> its latest facts
status = {"state": "starting", "polls": 0, "errors": 0, "vitals_at": 0, "signed_out_event_at": 0, "account": None,
          "caught_up": False, "caught_up_at": int(time.time() * 1000), "said_caught_up": None}
"""`caught_up`: whether the last look read the whole inbox and the message requests; `caught_up_at`: when a
look last did; `said_caught_up`: what the latest heartbeat said of it."""



def log(msg):
    print(msg, file=sys.stderr, flush=True)


def message_parts(body):
    """In Switch, who sent a conversation's latest message and what it says, for the inbox and Contacts."""
    f = body.get("facts") or {}
    by = f.get("last_from") or ""
    if not by or by in (f.get("account"), "me"):
        sender = {"handle": f.get("account") or "", "via": "instagram", "me": True}
    else:
        sender = {"handle": by.lower(), "via": "instagram"}
        name = next((p.get("name") for p in f.get("people") or [] if p.get("username") == by), None)
        if name:
            sender["name"] = name
    message = {"id": body.get("entity"), "conversation": f"instagram:{body.get('entity')}",
               "text": f.get("last_message") or "", "at": f.get("last_at"),
               "unread": bool(f.get("unread")) and not sender.get("me"), "channel": "instagram"}
    return sender, message


def record(kind, body, about, stream=STREAM, importance=None):
    """Sends what it saw as the event it is, for its keeper to record."""
    event = "heartbeat" if stream.startswith("vitals/") else "thread"
    if event == "thread":
        sender, message = message_parts(body)
        app.emit(event, body, about=about, importance=importance, sender=sender, message=message)
        return
    app.emit(event, body, about=about, importance=importance)


def now_ms():
    return int(time.time() * 1000)


def sb(*args, timeout=120):
    r = subprocess.run(["sb", *args], capture_output=True, text=True, timeout=timeout)
    return r.returncode, r.stdout, r.stderr


# The page, through the macOS appendage's browser.


class Paused(Exception):
    pass


class SignedOut(Exception):
    pass


def js(op):
    return {"op": "js", "code": PAGE_JS.replace("__IGDM_ARGS__", json.dumps(op))}


SWITCH = bool(os.environ.get("SWITCH_MACHINE"))
"""Run by Switch: the page is driven through the browser extension's act tool, in this extension's own session tab; else
through Switchboard's macOS appendage."""


def text_of(result):
    return "\n".join(c.get("text", "") for c in (result or {}).get("content") or [] if c.get("type") == "text")


def switch_steps(ops, timeout):
    """The same ops through the browser extension's act tool, in one call, as the macOS appendage's browser action took them."""
    r = app.call("browser_act", {"op": "steps", "session": SESSION, "steps": ops}, timeout=timeout + 15)
    if r.get("isError"):
        raise RuntimeError(text_of(r)[:400])
    return {"ok": True, "output": text_of(r)}


def steps(ops, timeout=120):
    """Runs browser ops as one request; returns every @@IGDM@@ answer in order."""
    if SWITCH:
        try:
            res = switch_steps(ops, timeout)
        except RuntimeError as e:
            res = {"ok": False, "error": str(e)}
        text = res.get("output") or res.get("error") or ""
        answers = [json.loads(m) for m in re.findall(r"@@IGDM@@(\{.*\})", text)]
        if any(a.get("signed_in") is False for a in answers):
            raise SignedOut()
        if not res.get("ok"):
            raise RuntimeError(res["error"][:400])
        return answers
    code, out, err = sb("request", BROWSER, "browser", "--args", json.dumps({"op": "steps", "session": SESSION, "steps": ops}),
                        "--timeout", str(timeout), timeout=timeout + 15)
    try:
        res = json.loads(out)
    except ValueError:
        raise RuntimeError((err or out or f"sb request exited {code}").strip()[:400])
    text = res.get("output") or res.get("error") or ""
    answers = [json.loads(m) for m in re.findall(r"@@IGDM@@(\{.*\})", text)]
    if any(a.get("signed_in") is False for a in answers):
        raise SignedOut()
    if not res.get("ok"):
        error = res.get("error") or "browser failed"
        if "pressed Cancel" in error:
            raise Paused(error)
        raise RuntimeError(error.split("\nDone before it:")[0][:400])
    return answers


def capture():
    """Learns the DM queries from the inbox page: each is caught the first time the app runs it."""
    log("capturing the DM queries from the inbox page")
    # From a fresh requests page into the inbox: the inbox, its account header and its threads' details.
    steps([{"op": "navigate", "url": BASE + "/direct/requests/"}, {"op": "wait", "seconds": 4},
           js({"op": "hook"}), js({"op": "go", "href": "/direct/inbox/"}), {"op": "wait", "seconds": 5},
           {"op": "navigate", "url": PARK}], timeout=90)
    # From a fresh inbox into requests: the message requests.
    answers = steps([{"op": "navigate", "url": BASE + "/direct/inbox/"}, {"op": "wait", "seconds": 4},
                     js({"op": "hook"}), js({"op": "go", "href": "/direct/requests/"}), {"op": "wait", "seconds": 4},
                     js({"op": "templates"}), {"op": "navigate", "url": PARK}], timeout=90)
    got = next((a.get("templates") for a in answers if "templates" in a), {}) or {}
    log(f"captured: {', '.join(f'{k} ({v['age_s']}s old)' for k, v in got.items()) or 'nothing'}")
    if "inbox" not in got:
        raise RuntimeError("the inbox page never ran its inbox query")
    return got


def page(op, timeout=60):
    """One page op from the parked tab, capturing the queries first when they're missing or stale."""
    for attempt in (1, 2):
        answers = steps([{"op": "navigate", "url": PARK}, js(op)], timeout=timeout)
        a = answers[-1] if answers else {}
        if not a.get("need_capture"):
            return a
        if attempt == 2:
            raise RuntimeError(f"{a.get('failed') or a.get('missing')}: {a.get('error') or 'not captured'}")
        log(f"replay needs a capture ({a.get('failed') or a.get('missing')}: {a.get('error')})")
        capture()


# Conversations.


def entity_of(t):
    return f"instagram-thread:{t['id']}"


def event_of(m):
    text = m.get("text") or ""
    if m.get("kind") == "SlideMessageAdminText" or re.match(r"^(Reacted .* to your message|Liked a message)", text):
        return "reaction"
    if re.search(r" sent (an attachment|a photo|a video|a voice message|a reel|a post|a sticker|a GIF)\.?$", text) or (
            m.get("kind") == "SlideMessageXMAContent" and not text):
        return "attachment"
    return "message"


def observe(t, me, backfill):
    """The conversation's new facts, or None when nothing changed."""
    key = entity_of(t)
    prev = convs.get(key)
    msgs = t["msgs"]
    if not msgs:
        return key, None
    last = msgs[-1]
    from_me = last["by"] == "me"
    theirs = [w for w in t["their_watermarks"] if w]
    seen_by_them = bool(theirs and max(theirs) >= last["at"]) if from_me else None
    unread = bool(t["marked_unread"] or (not from_me and last["at"] > (t["my_watermark"] or 0)))
    if prev and prev.get("last_id") == last["id"] and prev.get("unread") == unread and prev.get("seen_by_them") == seen_by_them:
        return key, None

    event = event_of(last)
    since = (prev or {}).get("last_at") or 0
    new = [m for m in msgs if m["by"] != "me" and m["at"] > since and event_of(m) != "reaction"]
    new_from_them = bool(not backfill and prev is not None and new) or bool(not backfill and prev is None and new and unread)
    unanswered = []
    for m in msgs:
        unanswered = [] if m["by"] == "me" else unanswered + ([m["text"]] if event_of(m) == "message" else [])
    waiting_on = None
    if not t["users"]:
        waiting_on = None  # the user's notes to himself
    elif from_me:
        waiting_on = "them"
    elif event == "message":
        waiting_on = "you"
    people = [{"username": u["username"], "name": u["name"]} for u in t["users"]]
    facts = {
        "account": me.get("username"),
        "title": t["title"],
        "with": [u["username"] for u in t["users"]],
        "people": people,
        "group": t["group"],
        "folder": t["folder"],
        "request": t["folder"] == "requests",
        "last_message": (last["text"] or "")[:600],
        "last_from": last["by"],
        "last_at": last["at"],
        "last_id": last["id"],
        "event": event,
        "unread": unread,
        "seen_by_them": seen_by_them,
        "waiting_on": waiting_on,
        "new_from_them": new_from_them,
        "new_messages": [{"by": m["by"], "text": m["text"], "at": m["at"]} for m in new] if new_from_them else None,
        "unanswered": unanswered[-8:],
        "messages": [{"by": m["by"], "text": (m["text"] or "")[:600], "at": m["at"]} for m in msgs[-KEEP:]],
        "muted": t["muted"],
        "thread_key": t["key"],
        "link": f"{BASE}/direct/t/{t['key'] or t['id']}/",
        "seen_at": now_ms(),
        "backfill": backfill or None,
    }
    return key, facts


def summary_of(f):
    who = "You" if f["last_from"] == "me" else f"@{f['last_from']}"
    said = f["last_message"].replace("\n", " ")
    if re.match(r"^You sent ", said):
        who = ""
    tail = ""
    if f["last_from"] != "me" and f["unread"]:
        tail = " (unread)"
    elif f["last_from"] == "me":
        tail = " (seen)" if f["seen_by_them"] else " (not seen yet)"
    return f"{who + ': ' if who else ''}{said[:160]}{tail}"


def name_of(f):
    handles = ", ".join("@" + w for w in f["with"][:3])
    if f["group"] or not handles:
        return f["title"] or handles or "Instagram chat"
    return f"{f['title']} ({handles})" if f["title"] and f["title"] != f["with"][0] else handles


_live = set()


def session_live(account):
    """The browser is signed in as this account: the user's Instagram connection says so, once."""
    if account in _live:
        return
    try:
        if not any(x.get("account") == account for x in app.accounts("instagram")):
            app.adopt("instagram", account, {})
            log(f"instagram: {account}'s sign-in is the connection")
        _live.add(account)
    except Exception as e:  # noqa: BLE001  (never stops a poll)
        log(f"instagram: couldn't record the sign-in: {e}")


def poll():
    with poll_lock:
        status["caught_up"] = False  # until this look reads everything
        backfill = not convs
        a = page({"op": "read", "requests": True})
        me = a.get("me") or {}
        status["account"] = me.get("username") or status["account"]
        if status["account"] and SWITCH:
            session_live(status["account"])
        if a.get("requests_error"):
            log(f"message requests: {a['requests_error']}")
        changed = 0
        for t in a.get("threads") or []:
            key, facts = observe(t, me, backfill)
            if facts is None:
                continue
            convs[key] = facts
            changed += 1
            record("observation", {"entity": key, "kind": "instagram-thread", "name": name_of(facts),
                                   "summary": summary_of(facts), "facts": facts},
                   [key], importance=0.7 if facts["new_from_them"] else 0.3)
        if changed:
            log(f"{changed} conversations changed" + (" (backfill)" if backfill else ""))
        status["caught_up"] = not a.get("requests_error")
        if status["caught_up"]:
            status["caught_up_at"] = now_ms()
        return "ok"


# State, from the brain: the latest facts of each conversation.


def load_state():
    code, out, err = sb("query", f'stream("{STREAM}") | where kind == "observation" | latest by body.entity | project entity = body.entity, facts = body.facts')
    if code != 0:
        raise RuntimeError(f"couldn't read {STREAM}: {(err or out).strip()[:300]}")
    for row in json.loads(out).get("rows", []):
        entity, facts = (row.get("entity"), row.get("facts")) if isinstance(row, dict) else row
        if entity and isinstance(facts, dict):
            convs.setdefault(entity, facts)  # what a look has read since is newer
    log(f"{len(convs)} conversations known from {STREAM}")


def interval():
    t = now_ms()
    live = any(
        (f.get("waiting_on") == "them" and t - (f.get("last_at") or 0) < LIVE_WAIT_H * 3600_000)
        or (f.get("last_from") != "me" and t - (f.get("last_at") or 0) < 30 * 60_000)
        for f in convs.values()
    )
    return (LIVE_S if live else NORMAL_S) * random.uniform(0.85, 1.15), live


def vitals(state, detail=None, live=False, event=None):
    t = now_ms()
    recent = [f for f in convs.values() if t - (f.get("last_at") or 0) < 7 * 86400_000]
    facts = {
        "state": state,
        "event": event,
        "account": status["account"],
        "checked_at": t,
        "conversations": len(convs),
        "recent": len(recent),
        "unread": len([f for f in recent if f.get("unread") and f.get("last_from") != "me"]),
        "waiting_on_them": len([f for f in recent if f.get("waiting_on") == "them"]),
        "live": live,
        "polls": status["polls"],
        "detail": detail,
        "behind_ms": 0 if status["caught_up"] else max(0, t - status["caught_up_at"]),
    }
    summary = {
        "ok": f"Watching {len(recent)} Instagram chats from this week; {facts['unread']} unread, {facts['waiting_on_them']} waiting on a reply",
        "signed_out": f"Instagram is signed out in Chrome on {HOST}",
        "paused": "Paused: Cancel was pressed on Chrome's debugging bar",
        "error": f"Can't read the Instagram inbox: {detail}",
    }.get(state, state)
    record("observation", {"entity": SENSE, "kind": "sense", "name": "Instagram DM watch", "summary": summary, "facts": facts},
           [SENSE], stream=VITALS)
    status["vitals_at"] = t
    status["said_caught_up"] = status["caught_up"]


def due():
    """A heartbeat is due: the last is HEARTBEAT_S old, or whether it's caught up changed since."""
    return now_ms() - status["vitals_at"] > HEARTBEAT_S * 1000 or status["caught_up"] != status["said_caught_up"]


def loop():
    try:
        load_state()
    except Exception as e:  # REVIEW: without state the first poll backfills quietly, so nothing wakes twice
        log(f"state: {e}")
    while True:
        wait_s, live = interval()
        try:
            state = poll()
            status["polls"] += 1
            if status["state"] != state or due():
                vitals(state, live=live)
            status["state"], status["errors"] = state, 0
        except SignedOut:
            # Only the change (and a reminder every few hours) says signed_out as an event, so a wake fires once.
            again = now_ms() - status["signed_out_event_at"] > SIGNED_OUT_AGAIN_H * 3600_000
            if status["state"] != "signed_out" or again:
                vitals("signed_out", event="signed_out")
                status["signed_out_event_at"] = now_ms()
            elif due():
                vitals("signed_out")
            status["state"] = "signed_out"
            wait_s = SIGNED_OUT_S
        except Paused as e:
            log(f"paused: {e}")
            if status["state"] != "paused" or due():
                vitals("paused", str(e)[:200])
            status["state"] = "paused"
            wait_s = CANCEL_PAUSE_S
        except Exception as e:  # REVIEW: one failed poll is retried with backoff; it never kills the sense
            status["errors"] += 1
            log(f"poll failed ({status['errors']}): {e}")
            if status["state"] != "error" or status["errors"] == 3 or due():
                vitals("error", str(e)[:200])
            status["state"] = "error"
            wait_s = min(60 * 2 ** (status["errors"] - 1), 900)
        wake.wait(wait_s)
        wake.clear()


# Actions.


def known(ref):
    """A known conversation by thread id, link, entity or @username, or None."""
    m = re.search(r"/direct/t/(\d+)", ref)
    tid = m.group(1) if m else ref.split(":", 1)[1] if ref.startswith("instagram-thread:") else ref
    if f"instagram-thread:{tid}" in convs:
        return f"instagram-thread:{tid}", convs[f"instagram-thread:{tid}"]
    for k, f in convs.items():  # the short id the web's links use
        if tid and f.get("thread_key") == tid:
            return k, f
    handle = ref.lstrip("@").lower()
    hits = [(k, f) for k, f in convs.items() if not f.get("group") and [w.lower() for w in f.get("with") or []] == [handle]]
    return max(hits, key=lambda kf: kf[1].get("last_at") or 0) if hits else None


def find(ref):
    """A known conversation by thread id, link, entity or @username."""
    ref = str(ref or "").strip()
    hit = known(ref)
    if hit is None:
        # Just started, the brain may not have answered yet (the core was still coming up): read it again
        # before refusing.
        try:
            load_state()
        except RuntimeError as e:
            log(f"state: {e}")
        hit = known(ref)
    if hit is not None:
        return hit
    raise ValueError(f"no known Instagram conversation matches {ref!r}; run check first, or start a new chat in the browser")


def brief(f):
    return {k: f.get(k) for k in ("title", "with", "folder", "last_from", "last_message", "last_at", "unread", "seen_by_them",
                                  "waiting_on", "event", "unanswered", "messages", "link")}


def act(action, args):
    if action == "check":
        state = poll()
        t = now_ms()
        days = float(args.get("days", 7))
        recent = sorted((f for f in convs.values() if t - (f.get("last_at") or 0) < days * 86400_000), key=lambda f: -(f.get("last_at") or 0))
        return {"state": state, "account": status["account"], "conversations": [brief(f) for f in recent]}
    if action == "thread":
        key, f = find(args.get("thread"))
        with poll_lock:
            a = page({"op": "thread", "key": f["thread_key"], "count": int(args.get("count") or 20)})
        th = a.get("thread")
        if not th:
            raise RuntimeError("Instagram returned no such thread")
        return {"title": th["title"], "with": [u["username"] for u in th["users"]], "link": f["link"],
                "unread": f.get("unread"), "seen_by_them": f.get("seen_by_them"),
                "messages": [{"by": m["by"], "text": m["text"], "at": datetime.datetime.fromtimestamp(m["at"] / 1000, datetime.timezone.utc).isoformat(timespec="seconds"), "kind": event_of(m)}
                             for m in th["msgs"]]}
    if action == "send":
        return send(args)
    raise ValueError(f"unknown action {action}")


def send(args):
    """Types a message into a conversation and sends it, then checks Instagram has it."""
    text = str(args.get("text") or "").strip()
    dry = bool(args.get("dry_run"))
    if not text and not dry:
        raise ValueError("nothing to send")
    key, f = find(args.get("thread"))
    ids = [key.split(":", 1)[1], f.get("thread_key")]
    url = f"{BASE}/direct/t/{f.get('thread_key') or ids[0]}/"
    lines = text.split("\n")
    typing = []
    for i, line in enumerate(lines):
        if i:
            typing.append({"op": "key", "keys": "shift+Enter"})
        if line:
            typing.append({"op": "type", "text": line, "keystrokes": True})
    norm = lambda s: re.sub(r"\s+", " ", s or "").strip()
    with poll_lock:
        try:
            answers = steps([{"op": "navigate", "url": url}, {"op": "wait", "seconds": 4}, js({"op": "compose", "ids": ids})], timeout=60)
            a = answers[-1] if answers else {}
            if not a.get("focused") or not a.get("on_thread"):
                raise RuntimeError(f"couldn't reach the message box in {url} (landed on {a.get('url')}, focused {a.get('focused')})")
            if dry:
                return {"sent": False, "dry_run": True, "ready": True, "to": f["with"], "landed_on": a.get("url")}
            steps(typing + [{"op": "wait", "seconds": 1}, {"op": "key", "keys": "Enter"}, {"op": "wait", "seconds": 3}], timeout=120)
        finally:
            # An open chat marks every new message read: never leave the tab on one.
            steps([{"op": "navigate", "url": PARK}], timeout=30)
        sent = None
        for _ in range(3):
            th = page({"op": "thread", "key": f["thread_key"], "count": 5}).get("thread") or {}
            mine = [m for m in th.get("msgs") or [] if m["by"] == "me"]
            sent = next((m for m in reversed(mine) if norm(m["text"]) == norm(text)), None)
            if sent:
                break
            time.sleep(3)
    wake.set()  # record the conversation as it now stands
    if not sent:
        raise RuntimeError(f"typed and pressed Enter, but Instagram doesn't show the message in {f['link']}: look before trying again")
    return {"sent": True, "to": f["with"], "text": sent["text"], "at": sent["at"], "link": f["link"]}



TOOLS = [
    {
        "name": "check",
        "description": "Read the Instagram inbox now (and message requests) and return the conversations active in the last few days, newest first: who, the latest message and who sent it, unread, whether they've seen the user's last message, what's unanswered, recent messages and a link. Never opens a chat.",
        "input_schema": {
            "type": "object",
            "properties": {
                "days": {
                    "type": "number",
                    "description": "How far back to list conversations (default 7)."
                }
            }
        }
    },
    {
        "name": "thread",
        "description": "One conversation's latest messages in full (default 20), without opening it, so it stays unread and they see no Seen.",
        "input_schema": {
            "type": "object",
            "properties": {
                "thread": {
                    "type": "string",
                    "description": "The other person's @username (a one-to-one chat), its instagram-thread:<id> entity, its id, or its link."
                },
                "count": {
                    "type": "integer",
                    "description": "How many messages (default 20)."
                }
            },
            "required": [
                "thread"
            ]
        }
    },
    {
        "name": "send",
        "description": "Send a DM in the user's name in an existing conversation, then confirm Instagram has it. It speaks for him: run it only when he asked for exactly this message, or from an approval ask's button with the words in a draft ({{draft:<name>}}). Opening the chat to send marks it read.",
        "input_schema": {
            "type": "object",
            "properties": {
                "thread": {
                    "type": "string",
                    "description": "The @username (a one-to-one chat), its instagram-thread:<id> entity, its id, or its link."
                },
                "text": {
                    "type": "string",
                    "description": "The message, exactly as approved. Line breaks are kept."
                },
                "dry_run": {
                    "type": "boolean",
                    "description": "Open the chat and reach its message box, then stop without typing or sending (a test; it still marks the chat read)."
                }
            },
            "required": [
                "thread"
            ]
        }
    }
]
READ_ONLY = ['check', 'thread']
SPEAKS_FOR_PERSON = ['send']


def tool(name):
    def call(**args):
        try:
            return act(name, args)
        except SignedOut:
            raise RuntimeError(f"Instagram is signed out in Chrome on {HOST}") from None
    return call


for spec in TOOLS:
    app.tool(tool(spec["name"]), name=spec["name"], description=spec["description"], schema=spec["input_schema"],
             read_only=spec["name"] in READ_ONLY, speaks_for_person=spec["name"] in SPEAKS_FOR_PERSON)

app.background(loop)


if __name__ == "__main__":
    app.run()
