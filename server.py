"""
NHL Goal Light — Flask server + subprocess manager
===================================================
Run: python server.py
Then open http://localhost:5000

Dependencies:
    pip install flask aiohttp python-kasa
"""
from __future__ import annotations
import asyncio
import concurrent.futures
import os
import queue
import subprocess
import sys
import threading
import time
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from flask import Flask, Response, jsonify, request, send_from_directory, stream_with_context

# ── optional: fall back to urllib if requests isn't installed ─────────────────
try:
    import requests as _http_lib
    def _fetch_json(url: str, timeout: int = 10) -> dict:
        r = _http_lib.get(url, timeout=timeout)
        r.raise_for_status()
        return r.json()
except ImportError:
    import urllib.request, json as _json
    def _fetch_json(url: str, timeout: int = 10) -> dict:
        with urllib.request.urlopen(url, timeout=timeout) as resp:
            return _json.loads(resp.read())

# ─────────────────────────────────────────────────────────────────────────────
HERE = os.path.dirname(os.path.abspath(__file__))
MAIN_SCRIPT = os.path.join(HERE, "main.py")
NHL_API_BASE = "https://api-web.nhle.com/v1"
EASTERN_TZ = ZoneInfo("America/New_York")

app = Flask(__name__, static_folder=HERE)

# Import main for access to classes and constants
import main

# ── subprocess state ──────────────────────────────────────────────────────────
_proc: subprocess.Popen | None = None
_proc_lock = threading.Lock()
_log_queue: queue.Queue[str | None] = queue.Queue()

# ── persistent log history (survives page refresh / multi-device) ─────────────
_log_history: list[str] = []
_log_history_lock = threading.Lock()
MAX_LOG_HISTORY = 2000

# ── games cache ───────────────────────────────────────────────────────────────
_games_cache: list[dict] | None = None
_games_cache_ts: float = 0.0
_GAMES_CACHE_TTL = 60.0  # seconds

ENV_MAP = {
    "bulb_ip": "BULB_IP",
    "flash_duration": "FLASH_DURATION",
    "flash_interval": "FLASH_INTERVAL",
    "flash_quiet_window": "FLASH_QUIET_WINDOW",
    "flash_transition_ms": "FLASH_TRANSITION_MS",
    "poll_live_seconds": "POLL_LIVE_SECONDS",
    "poll_critical_seconds": "POLL_CRITICAL_SECONDS",
    "poll_pregame_seconds": "POLL_PREGAME_SECONDS",
    "poll_error_seconds": "POLL_ERROR_SECONDS",
    "request_timeout": "NHL_REQUEST_TIMEOUT",
    "max_retries": "NHL_MAX_RETRIES",
    "backoff_base": "NHL_BACKOFF_BASE",
    "backoff_max": "NHL_BACKOFF_MAX",
    "pregame_buffer_seconds": "PREGAME_BUFFER_SECONDS",
    "restore_transition_ms": "RESTORE_TRANSITION_MS",
    "goal_delay_seconds": "GOAL_DELAY_SECONDS",
}

# ── helpers ───────────────────────────────────────────────────────────────────
def _format_local_time(utc_str: str) -> str:
    dt = datetime.strptime(utc_str, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    local_tz = datetime.now().astimezone().tzinfo or timezone.utc
    return dt.astimezone(local_tz).strftime("%I:%M %p")

def _read_stdout(proc: subprocess.Popen) -> None:
    """Drain proc.stdout into _log_queue AND append to persistent _log_history."""
    global _log_history
    try:
        for raw in proc.stdout:
            line = raw.decode(errors="replace").rstrip()
            _log_queue.put(line)
            with _log_history_lock:
                _log_history.append(line)
                if len(_log_history) > MAX_LOG_HISTORY:
                    _log_history = _log_history[-MAX_LOG_HISTORY:]
    except Exception:
        pass
    finally:
        _log_queue.put(None)

def _proc_is_alive() -> bool:
    global _proc
    return _proc is not None and _proc.poll() is None

# ── routes ────────────────────────────────────────────────────────────────────
@app.route("/")
def index():
    return send_from_directory(HERE, "web.html")

@app.route("/api/games")
def api_games():
    global _games_cache, _games_cache_ts
    now = time.monotonic()
    if _games_cache is not None and (now - _games_cache_ts) < _GAMES_CACHE_TTL:
        return jsonify(_games_cache)

    today = datetime.now(EASTERN_TZ).date().isoformat()
    try:
        payload = _fetch_json(f"{NHL_API_BASE}/schedule/{today}")
    except Exception as exc:
        return jsonify({"error": str(exc)}), 502

    games: list[dict] = []
    for day in payload.get("gameWeek", []):
        if day.get("date") != today:
            continue
        for g in day.get("games", []):
            away = g.get("awayTeam", {})
            home = g.get("homeTeam", {})

            def _name(team: dict) -> str:
                place = team.get("placeName", {}).get("default", "")
                common = team.get("commonName", {}).get("default", "")
                return f"{place} {common}".strip()

            utc_str = g.get("startTimeUTC", "1970-01-01T00:00:00Z")
            games.append({
                "id": g["id"],
                "away_abbrev": away.get("abbrev", "???"),
                "away_name": _name(away),
                "home_abbrev": home.get("abbrev", "???"),
                "home_name": _name(home),
                "start_time_utc": utc_str,
                "game_state": g.get("gameState", ""),
                "start_time_local": _format_local_time(utc_str),
            })

    games.sort(key=lambda g: g["start_time_utc"])
    _games_cache = games
    _games_cache_ts = now
    return jsonify(games)

@app.route("/api/game-state/<game_id>")
def api_game_state(game_id: str):
    """Return live game state: period, clock, score, intermission status."""
    try:
        data = _fetch_json(f"{NHL_API_BASE}/gamecenter/{game_id}/landing")
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 502

    state = data.get("gameState", "")
    clock_data = data.get("clock", {})
    period_desc = data.get("periodDescriptor", {})
    away_team = data.get("awayTeam", {})
    home_team = data.get("homeTeam", {})

    return jsonify({
        "ok": True,
        "state": state,
        "period": period_desc.get("number"),
        "period_type": period_desc.get("periodType", "REG"),
        "clock": clock_data.get("timeRemaining"),
        "in_intermission": clock_data.get("inIntermission", False),
        "away_abbrev": away_team.get("abbrev", ""),
        "away_score": away_team.get("score", 0),
        "home_abbrev": home_team.get("abbrev", ""),
        "home_score": home_team.get("score", 0),
    })

@app.route("/api/log-history")
def api_log_history():
    """Return all persistent log entries so clients can load history on connect."""
    with _log_history_lock:
        return jsonify({"entries": list(_log_history)})

@app.route("/api/clear-log", methods=["POST"])
def api_clear_log():
    """Clear the persistent log history server-side."""
    global _log_history
    with _log_history_lock:
        _log_history = []
    return jsonify({"ok": True})

@app.route("/api/last-goal-time")
def api_last_goal_time():
    """Return the API's best estimate of when the last goal was scored."""
    game_id = request.args.get("game_id", "").strip()
    if not game_id:
        return jsonify({"ok": False, "error": "game_id required"}), 400
    try:
        pbp = _fetch_json(f"{NHL_API_BASE}/gamecenter/{game_id}/play-by-play")
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 502

    plays = pbp.get("plays", [])
    goals = [p for p in plays if p.get("typeDescKey") == "goal"]
    if not goals:
        return jsonify({"ok": False, "error": "No goals in this game yet."}), 404

    last_goal = goals[-1]
    period    = last_goal.get("periodDescriptor", {}).get("number", 1)
    tip       = last_goal.get("timeInPeriod", "0:00")
    scorer    = last_goal.get("details", {}).get("scoringPlayerId", "")

    # Compute wall-clock offset using startTimeUTC + period offsets
    start_utc_str = pbp.get("startTimeUTC", "")
    try:
        game_start_dt = datetime.strptime(start_utc_str[:19], "%Y-%m-%dT%H:%M:%S").replace(tzinfo=timezone.utc)
    except Exception:
        return jsonify({"ok": False, "error": "Cannot parse game start time."}), 500

    try:
        tp = tip.split(":")
        elapsed_in_period = int(tp[0]) * 60 + int(tp[1])
    except Exception:
        return jsonify({"ok": False, "error": "Cannot parse goal time."}), 500

    period_length    = 300 if period >= 4 else 1200
    INTERMISSION     = 1020
    real_offset      = (period - 1) * (period_length + INTERMISSION) + elapsed_in_period
    goal_est_dt      = game_start_dt + __import__('datetime').timedelta(seconds=real_offset)

    return jsonify({
        "ok": True,
        "goal_period": period,
        "goal_time": tip,
        "goal_est_utc": goal_est_dt.isoformat(),
        "scorer_id": scorer,
    })

@app.route("/api/calibrate-tap", methods=["POST"])
def api_calibrate_tap():
    """
    User taps this endpoint the instant they see a goal on their TV.
    Server computes delay = now() - estimated_goal_utc.
    Body: { "game_id": "...", "goal_est_utc": "..." }
    """
    body        = request.get_json(force=True) or {}
    game_id     = body.get("game_id", "")
    goal_utc    = body.get("goal_est_utc", "")

    if not game_id or not goal_utc:
        return jsonify({"ok": False, "error": "game_id and goal_est_utc required"}), 400

    try:
        goal_dt = datetime.strptime(goal_utc[:19], "%Y-%m-%dT%H:%M:%S").replace(tzinfo=timezone.utc)
    except Exception:
        return jsonify({"ok": False, "error": "Invalid goal_est_utc"}), 400

    now_utc = datetime.now(timezone.utc)
    delay   = round((now_utc - goal_dt).total_seconds())

    if delay < 0:
        return jsonify({"ok": False, "error": f"Negative delay ({delay}s) — tap AFTER you see the goal on TV."}), 400
    if delay > 600:
        return jsonify({"ok": False, "error": f"Delay too large ({delay}s) — did you tap on the right goal?"}), 400

    return jsonify({"ok": True, "delay_seconds": delay})

@app.route("/api/calibrate-delay")
def api_calibrate_delay():
    """
    Fallback: compute TV delay by comparing current clock on TV vs API.
    (Primary method is now goal-tap for better accuracy.)

    Query params:
      game_id – NHL game ID
      period  – period number the TV is showing (1-4)
      clock   – time REMAINING as MM:SS shown on TV (e.g. 16:15)
    """
    game_id    = request.args.get("game_id",  "").strip()
    period_str = request.args.get("period",   "").strip()
    clock_str  = request.args.get("clock",    "").strip()

    if not game_id:
        return jsonify({"ok": False, "error": "game_id required"}), 400
    if not period_str:
        return jsonify({"ok": False, "error": "period required"}), 400
    if not clock_str:
        return jsonify({"ok": False, "error": "clock must be MM:SS format"}), 400

    try:
        period = int(period_str)
    except ValueError:
        return jsonify({"ok": False, "error": "period must be a number"}), 400

    try:
        parts = clock_str.split(":")
        tv_remaining = int(parts[0]) * 60 + int(parts[1])
    except Exception:
        return jsonify({"ok": False, "error": "clock must be MM:SS format"}), 400

    # Fetch the live game state — this gives us the API's current clock
    try:
        data = _fetch_json(f"{NHL_API_BASE}/gamecenter/{game_id}/landing")
    except Exception as exc:
        return jsonify({"ok": False, "error": f"NHL API error: {exc}"}), 502

    state = data.get("gameState", "")
    if state not in ("LIVE", "CRIT"):
        return jsonify({"ok": False, "error": f"Game is not live (state={state}). Calibration requires a live game."}), 400

    clock_data = data.get("clock", {})
    period_desc = data.get("periodDescriptor", {})

    api_clock_str = clock_data.get("timeRemaining", "")
    api_period    = period_desc.get("number")
    in_intermission = clock_data.get("inIntermission", False)

    if in_intermission:
        return jsonify({"ok": False, "error": "Game is in intermission. Wait for the next period to start."}), 400

    if not api_clock_str:
        return jsonify({"ok": False, "error": "Could not read API clock. Try again."}), 500

    try:
        api_parts = api_clock_str.split(":")
        api_remaining = int(api_parts[0]) * 60 + int(api_parts[1])
    except Exception:
        return jsonify({"ok": False, "error": f"Could not parse API clock: {api_clock_str}"}), 500

    # If the API is in a later period than TV, account for the full period difference
    period_length = 300 if period >= 4 else 1200
    period_diff = (api_period - period) * period_length

    # Delay = how far behind the TV is vs the API right now
    delay = (tv_remaining - api_remaining) + period_diff

    if delay < 0:
        return jsonify({
            "ok": False,
            "error": f"Computed delay is negative ({delay}s) — enter your TV clock, not the API clock."
        }), 400

    if delay > 600:
        return jsonify({
            "ok": False,
            "error": f"Delay over 10 min ({delay}s) — check period and clock."
        }), 400

    return jsonify({
        "ok": True,
        "delay_seconds": delay,
        "play_type": "live-clock-sync",
        "play_clock": api_clock_str,
        "debug": {
            "tv_period": period,
            "tv_clock": clock_str,
            "tv_remaining": tv_remaining,
            "api_period": api_period,
            "api_clock": api_clock_str,
            "api_remaining": api_remaining,
            "period_diff_secs": period_diff,
        }
    })

@app.route("/api/start", methods=["POST"])
def api_start():
    global _proc
    with _proc_lock:
        if _proc_is_alive():
            return jsonify({"error": "Already monitoring. Stop first."}), 409

        body: dict = request.get_json(force=True) or {}
        if not body.get("bulb_ip", "").strip():
            return jsonify({"error": "bulb_ip is required"}), 400

        game_ids: list = body.get("game_ids", [])
        if not game_ids:
            return jsonify({"error": "At least one game_id is required"}), 400

        env = os.environ.copy()
        for json_key, env_key in ENV_MAP.items():
            if json_key in body and body[json_key] is not None:
                env[env_key] = str(body[json_key])

        env["GAME_IDS"] = ",".join(str(gid) for gid in game_ids)

        per_game_delays = body.get("per_game_delays", [])
        if per_game_delays:
            env["PER_GAME_DELAYS"] = ",".join(str(d) for d in per_game_delays)

        while not _log_queue.empty():
            try:
                _log_queue.get_nowait()
            except queue.Empty:
                break

        try:
            _proc = subprocess.Popen(
                [sys.executable, MAIN_SCRIPT],
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                env=env,
            )
        except Exception as exc:
            return jsonify({"error": f"Failed to start subprocess: {exc}"}), 500

        t = threading.Thread(target=_read_stdout, args=(_proc,), daemon=True)
        t.start()
        return jsonify({"ok": True, "pid": _proc.pid})

@app.route("/api/stop", methods=["POST"])
def api_stop():
    global _proc
    with _proc_lock:
        if not _proc_is_alive():
            return jsonify({"ok": True, "message": "No process running"})
        try:
            _proc.terminate()
        except Exception:
            pass
        try:
            _proc.wait(timeout=3)
        except subprocess.TimeoutExpired:
            try:
                _proc.kill()
            except Exception:
                pass
        _proc = None
        return jsonify({"ok": True})

@app.route("/api/status")
def api_status():
    with _proc_lock:
        alive = _proc_is_alive()
        pid = _proc.pid if alive else None
    return jsonify({
        "state": "monitoring" if alive else "idle",
        "pid": pid,
    })

@app.route("/api/log")
def api_log():
    def generate():
        while True:
            try:
                line = _log_queue.get(timeout=30)
            except queue.Empty:
                yield ": heartbeat\n\n"
                continue
            if line is None:
                yield "data: [DONE]\n\n"
                return
            safe = line.replace("\n", " ")
            yield f"data: {safe}\n\n"

    return Response(
        stream_with_context(generate()),
        mimetype="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        },
    )

@app.route("/api/test-bulb")
def api_test_bulb():
    from kasa.iot import IotBulb
    ip = request.args.get("ip", "").strip()
    if not ip:
        return jsonify({"ok": False, "model": None, "error": "No IP provided"})

    def _test_sync(ip: str) -> str:
        async def _inner():
            bulb = IotBulb(ip)
            await bulb.update()
            return bulb.model
        return asyncio.run(_inner())

    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
            model = pool.submit(_test_sync, ip).result(timeout=10)
        return jsonify({"ok": True, "model": model, "error": None})
    except Exception as exc:
        return jsonify({"ok": False, "model": None, "error": str(exc)})

@app.route("/api/team-colors")
def api_team_colors():
    out = {}
    for abbrev, palette in main.TEAM_COLORS.items():
        out[abbrev] = {
            "primary": list(palette.primary),
            "secondary": list(palette.secondary),
        }
    return jsonify(out)

@app.route("/api/simulate-goal")
def api_simulate_goal():
    try:
        ip = request.args.get("ip", "").strip()
        team = request.args.get("team", "").strip().upper()
        flash_duration = float(request.args.get("flash_duration", "12"))
        flash_interval = float(request.args.get("flash_interval", "0.45"))
        flash_transition_ms = int(request.args.get("flash_transition_ms", "0"))

        if not ip:
            return jsonify({"ok": False, "error": "No IP provided"})
        if not team:
            return jsonify({"ok": False, "error": "No team provided"})
        if team not in main.TEAM_COLORS:
            return jsonify({"ok": False, "error": f"Unknown team: {team}"})

        config = main.AppConfig(
            bulb_ip=ip,
            request_timeout=10.0,
            max_retries=4,
            backoff_base=1.0,
            backoff_max=30.0,
            flash_duration=flash_duration,
            flash_interval=flash_interval,
            flash_quiet_window=1.5,
            pregame_buffer_seconds=300,
            poll_live_seconds=1.0,
            poll_critical_seconds=0.75,
            poll_pregame_seconds=10.0,
            poll_error_seconds=5.0,
            restore_transition_ms=150,
            flash_transition_ms=flash_transition_ms,
            goal_delay_seconds=0,
        )

        async def _simulate():
            bulb = main.BulbController(config)
            snapshot = await bulb.capture_state()
            await bulb.flash_team(team, snapshot)
            await main._safe_restore(bulb, snapshot)
            await bulb.shutdown()

        def _run_sync():
            asyncio.run(_simulate())

        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
            pool.submit(_run_sync).result(timeout=60)
        return jsonify({"ok": True, "error": None})
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)})

# ── entry point ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    port = int(os.getenv("SERVER_PORT", "5000"))
    print(f"NHL Goal Light GUI → http://localhost:{port}")
    app.run(host="0.0.0.0", port=port, debug=False, threaded=True)
