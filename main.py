"""
NHL Goal Light Controller
━━━━━━━━━━━━━━━━━━━━━━━━
Monitors NHL games in real-time and flashes a TP-Link Kasa smart bulb
with team colors whenever a goal is scored.
"""

from __future__ import annotations

import asyncio
import logging
import os
import random
import signal
import traceback
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from typing import Optional
from zoneinfo import ZoneInfo

from kasa.iot import IotBulb

log = logging.getLogger(__name__)

NHL_API_BASE = "https://api-web.nhle.com/v1"
EASTERN_TZ = ZoneInfo("America/New_York")

FINAL_STATES = frozenset({"FINAL", "OFF"})
LIVE_STATES = frozenset({"LIVE", "CRIT"})
PREGAME_STATES = frozenset({"PRE", "FUT"})
RETRYABLE_STATUS = frozenset({429, 500, 502, 503, 504})
STALE_THRESHOLD = timedelta(seconds=30)


@dataclass(frozen=True)
class TeamPalette:
    primary: tuple[int, int, int]
    secondary: tuple[int, int, int]


TEAM_COLORS: dict[str, TeamPalette] = {
    "ANA": TeamPalette((18,  99,  99),  (196,  6, 68)),   # Orange, Silver
    "BOS": TeamPalette((42,  92,  99),  (0,    0,  7)),   # Gold, Black
    "BUF": TeamPalette((219, 100, 53),  (42,  92, 99)),   # Navy, Gold
    "CGY": TeamPalette((352, 100, 82),  (42,  92, 99)),   # Red, Gold
    "CAR": TeamPalette((353,  92, 81),  (0,    0,  7)),   # Red, Black
    "CHI": TeamPalette((350,  95, 81),  (0,    0,  7)),   # Red, Black
    "COL": TeamPalette((341,  66, 44),  (206, 76, 57)),   # Burgundy, Blue
    "CBJ": TeamPalette((213, 100, 33),  (353,  92, 81)),  # Navy, Red
    "DAL": TeamPalette((161, 100, 41),  (0,    0,  7)),   # Green, Black
    "DET": TeamPalette((353,  92, 81),  (0,    0, 100)),  # Red, White
    "EDM": TeamPalette((18,   99, 99),  (215,  94, 26)),  # Orange, Navy
    "FLA": TeamPalette((215,  94, 26),  (350,  92, 78)),  # Navy, Red
    "LAK": TeamPalette((0,     0,  7),  (196,   6, 68)),  # Black, Silver
    "MIN": TeamPalette((157,  70, 28),  (350,  85, 65)),  # Green, Red
    "MTL": TeamPalette((354,  83, 69),  (223,  82, 54)),  # Red, Blue
    "NSH": TeamPalette((41,   89, 100), (215,  94, 26)),  # Gold, Navy
    "NJD": TeamPalette((353,  92, 81),  (0,    0,  7)),   # Red, Black
    "NYI": TeamPalette((208, 100, 61),  (18,   99, 99)),  # Blue, Orange
    "NYR": TeamPalette((220, 100, 66),  (353,  92, 81)),  # Blue, Red
    "OTT": TeamPalette((353,  84, 77),  (37,   52, 72)),  # Red, Gold
    "PHI": TeamPalette((17,   99, 97),  (0,    0,  7)),   # Orange, Black
    "PIT": TeamPalette((0,     0,  7),  (42,   92, 99)),  # Black, Gold
    "SJS": TeamPalette((184, 100, 46),  (29,  100, 92)),  # Teal, Orange
    "SEA": TeamPalette((207, 100, 16),  (180,  29, 85)),  # Deep Navy, Ice Blue
    "STL": TeamPalette((219, 100, 53),  (42,   92, 99)),  # Blue, Gold
    "TBL": TeamPalette((217, 100, 41),  (0,    0, 100)),  # Blue, White
    "TOR": TeamPalette((219, 100, 36),  (0,    0, 100)),  # Blue, White
    "UTA": TeamPalette((216,  99, 31),  (197,  44, 73)),  # Navy, Teal
    "VAN": TeamPalette((219, 100, 36),  (156, 100, 53)),  # Blue, Green
    "VGK": TeamPalette((41,   50, 71),  (206,  29, 28)),  # Muted Gold, Steel Grey
    "WSH": TeamPalette((350,  85, 77),  (215,  94, 26)),  # Red, Navy
    "WPG": TeamPalette((215,  94, 26),  (210,  43, 38)),  # Navy, Aviator Blue
}

DEFAULT_PALETTE = TeamPalette((0, 100, 100), (0, 0, 100))


@dataclass(frozen=True)
class AppConfig:
    bulb_ip: str
    request_timeout: float
    max_retries: int
    backoff_base: float
    backoff_max: float
    flash_duration: float
    flash_interval: float
    flash_quiet_window: float
    pregame_buffer_seconds: int
    poll_live_seconds: float
    poll_critical_seconds: float
    poll_pregame_seconds: float
    poll_error_seconds: float
    restore_transition_ms: int
    flash_transition_ms: int
    goal_delay_seconds: int


@dataclass(frozen=True)
class GameInfo:
    game_id: int
    away_abbrev: str
    home_abbrev: str
    away_name: str
    home_name: str
    start_time_utc: datetime
    game_state: str


@dataclass(frozen=True)
class GameStatus:
    away_abbrev: str
    home_abbrev: str
    game_state: str
    away_score: Optional[int]
    home_score: Optional[int]


@dataclass(frozen=True)
class BulbSnapshot:
    is_on: bool
    brightness: int
    hue: int
    saturation: int
    color_temp: Optional[int]
    color_mode: Optional[str]
    supports_color: bool
    supports_color_temp: bool


@dataclass(frozen=True)
class GoalEvent:
    game_id: int
    scoring_team: str


class _RetryableError(Exception):
    def __init__(self, status: int, retry_after: Optional[float]) -> None:
        super().__init__(f"Retryable HTTP status: {status}")
        self.status = status
        self.retry_after = retry_after


def build_config() -> AppConfig:
    bulb_ip = os.getenv("BULB_IP", "").strip()
    if not bulb_ip or bulb_ip.upper() == "BULB IP HERE":
        raise SystemExit(
            "ERROR: Set the BULB_IP environment variable to your bulb's IP address before running.\n"
            "export BULB_IP=192.168.1.x"
        )

    return AppConfig(
        bulb_ip=bulb_ip,
        request_timeout=float(os.getenv("NHL_REQUEST_TIMEOUT", "10")),
        max_retries=int(os.getenv("NHL_MAX_RETRIES", "4")),
        backoff_base=float(os.getenv("NHL_BACKOFF_BASE", "1.0")),
        backoff_max=float(os.getenv("NHL_BACKOFF_MAX", "30.0")),
        flash_duration=float(os.getenv("FLASH_DURATION", "12")),
        flash_interval=float(os.getenv("FLASH_INTERVAL", "0.45")),
        flash_quiet_window=float(os.getenv("FLASH_QUIET_WINDOW", "1.5")),
        pregame_buffer_seconds=int(os.getenv("PREGAME_BUFFER_SECONDS", "300")),
        poll_live_seconds=float(os.getenv("POLL_LIVE_SECONDS", "1.0")),
        poll_critical_seconds=float(os.getenv("POLL_CRITICAL_SECONDS", "0.75")),
        poll_pregame_seconds=float(os.getenv("POLL_PREGAME_SECONDS", "10")),
        poll_error_seconds=float(os.getenv("POLL_ERROR_SECONDS", "5")),
        restore_transition_ms=int(os.getenv("RESTORE_TRANSITION_MS", "150")),
        flash_transition_ms=int(os.getenv("FLASH_TRANSITION_MS", "120")),
        goal_delay_seconds=int(os.getenv("GOAL_DELAY_SECONDS", "35")),
    )


def _format_local_time(dt_utc: datetime) -> str:
    local_tz = datetime.now().astimezone().tzinfo or timezone.utc
    return dt_utc.astimezone(local_tz).strftime("%I:%M %p")


def _parse_utc(value: str) -> datetime:
    return datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)


def _display_team_name(team: dict) -> str:
    place = team.get("placeName", {}).get("default", "")
    common = team.get("commonName", {}).get("default", "")
    return f"{place} {common}".strip()


def _extract_hsv(hsv_obj) -> tuple[int, int, int]:
    if hsv_obj is None:
        return 0, 0, 100
    try:
        return int(hsv_obj.hue), int(hsv_obj.saturation), int(hsv_obj.value)
    except AttributeError:
        pass
    try:
        if len(hsv_obj) >= 3:
            return int(hsv_obj[0]), int(hsv_obj[1]), int(hsv_obj[2])
    except Exception:
        pass
    return 0, 0, 100


def _clamp(value: int, lo: int, hi: int) -> int:
    return max(lo, min(hi, int(value)))


def _poll_interval(game_state: str, config: AppConfig) -> float:
    if game_state == "CRIT":
        return config.poll_critical_seconds
    if game_state in LIVE_STATES:
        return config.poll_live_seconds
    if game_state in FINAL_STATES:
        return 0.0
    return config.poll_pregame_seconds


def _format_schedule_state(game_state: str, local_time: str) -> str:
    if game_state in LIVE_STATES:
        return " [LIVE]"
    if game_state in FINAL_STATES:
        return " [FINAL]"
    if game_state in PREGAME_STATES:
        return f" [Starts {local_time}]"
    return f" [{game_state or 'UNKNOWN'} at {local_time}]"


def _backoff(attempt: int, base: float, maximum: float) -> float:
    ceiling = min(maximum, base * (2 ** attempt))
    return random.uniform(0, ceiling)


def _parse_retry_after(value: Optional[str]) -> Optional[float]:
    if not value:
        return None
    try:
        return max(0.0, float(value))
    except ValueError:
        pass
    try:
        retry_dt = parsedate_to_datetime(value)
        if retry_dt.tzinfo is None:
            retry_dt = retry_dt.replace(tzinfo=timezone.utc)
        delta = (retry_dt - datetime.now(timezone.utc)).total_seconds()
        return max(0.0, delta)
    except Exception:
        return None


async def _interruptible_sleep(seconds: float, shutdown: asyncio.Event) -> None:
    if seconds <= 0:
        return
    try:
        await asyncio.wait_for(shutdown.wait(), timeout=seconds)
    except asyncio.TimeoutError:
        pass


class NhlApiClient:
    def __init__(self, config: AppConfig) -> None:
        self._config = config
        self._session = None

    async def __aenter__(self) -> "NhlApiClient":
        import aiohttp
        self._session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=self._config.request_timeout)
        )
        return self

    async def __aexit__(self, *_) -> None:
        if self._session is not None:
            await self._session.close()
            self._session = None

    async def _get_json(self, path: str) -> dict:
        import aiohttp
        if self._session is None:
            raise RuntimeError("NhlApiClient must be used as an async context manager.")

        url = f"{NHL_API_BASE}{path}"
        last_exc: Exception = RuntimeError("No attempts made.")

        for attempt in range(self._config.max_retries):
            try:
                async with self._session.get(url) as resp:
                    if resp.status == 404:
                        return {}
                    if 400 <= resp.status < 500 and resp.status not in RETRYABLE_STATUS:
                        resp.raise_for_status()
                    if resp.status in RETRYABLE_STATUS:
                        retry_after = _parse_retry_after(resp.headers.get("Retry-After"))
                        raise _RetryableError(resp.status, retry_after)
                    resp.raise_for_status()
                    return await resp.json()
            except asyncio.CancelledError:
                raise
            except _RetryableError as exc:
                last_exc = exc
                wait = exc.retry_after if exc.retry_after is not None else _backoff(
                    attempt, self._config.backoff_base, self._config.backoff_max
                )
                log.warning("NHL API %s on %s (attempt %d/%d) — retrying in %.1fs.",
                            exc.status, url, attempt + 1, self._config.max_retries, wait)
                await asyncio.sleep(wait)
            except aiohttp.ClientError as exc:
                last_exc = exc
                wait = _backoff(attempt, self._config.backoff_base, self._config.backoff_max)
                log.warning("Network error on %s (attempt %d/%d): %s — retrying in %.1fs.",
                            url, attempt + 1, self._config.max_retries, exc, wait)
                await asyncio.sleep(wait)

        log.error("Giving up on %s after %d attempts.", url, self._config.max_retries)
        raise last_exc

    async def fetch_todays_games(self) -> list[GameInfo]:
        today = datetime.now(EASTERN_TZ).date().isoformat()
        payload = await self._get_json(f"/schedule/{today}")
        games: list[GameInfo] = []
        for day in payload.get("gameWeek", []):
            if day.get("date") != today:
                continue
            for g in day.get("games", []):
                games.append(GameInfo(
                    game_id=g["id"],
                    away_abbrev=g["awayTeam"]["abbrev"],
                    home_abbrev=g["homeTeam"]["abbrev"],
                    away_name=_display_team_name(g["awayTeam"]),
                    home_name=_display_team_name(g["homeTeam"]),
                    start_time_utc=_parse_utc(g["startTimeUTC"]),
                    game_state=g.get("gameState", ""),
                ))
        games.sort(key=lambda game: game.start_time_utc)
        return games

    async def fetch_game_status(self, game_id: int) -> Optional[GameStatus]:
        payload = await self._get_json(f"/gamecenter/{game_id}/boxscore")
        if not payload:
            return None
        away = payload.get("awayTeam", {})
        home = payload.get("homeTeam", {})
        return GameStatus(
            away_abbrev=away.get("abbrev", "???"),
            home_abbrev=home.get("abbrev", "???"),
            game_state=payload.get("gameState", ""),
            away_score=away.get("score"),
            home_score=home.get("score"),
        )


class BulbController:
    def __init__(self, config: AppConfig) -> None:
        self._config = config
        self._bulb: Optional[IotBulb] = None
        self._light = None
        self._io_lock = asyncio.Lock()
        self._connected = False
        self._last_updated = datetime.min.replace(tzinfo=timezone.utc)

    async def _ensure_connected(self) -> None:
        now = datetime.now(timezone.utc)
        needs_refresh = (
            not self._connected
            or self._bulb is None
            or (now - self._last_updated) > STALE_THRESHOLD
        )
        if needs_refresh:
            self._bulb = IotBulb(self._config.bulb_ip)
            await self._bulb.update()
            self._light = self._bulb.modules.get("Light")
            if self._light is None:
                raise RuntimeError(
                    f"Bulb at {self._config.bulb_ip} has no Light module."
                )
            self._connected = True
            self._last_updated = now

    async def _reconnect(self) -> None:
        self._connected = False
        await self._ensure_connected()

    async def _set_hsv_safe(self, hue: int, saturation: int, value: int, transition_ms: int) -> None:
        try:
            await self._light.set_hsv(hue, saturation, value, transition=transition_ms)
        except TypeError:
            await self._light.set_hsv(hue, saturation, value)

    async def _set_brightness_safe(self, brightness: int, transition_ms: int) -> None:
        try:
            await self._light.set_brightness(brightness, transition=transition_ms)
        except TypeError:
            await self._light.set_brightness(brightness)

    async def _set_color_temp_safe(self, color_temp: int, brightness: int, transition_ms: int) -> None:
        try:
            await self._light.set_color_temp(color_temp, brightness=brightness, transition=transition_ms)
        except TypeError:
            try:
                await self._light.set_color_temp(color_temp, brightness=brightness)
            except TypeError:
                await self._light.set_color_temp(color_temp)

    async def _turn_off_instant(self) -> None:
        """Turn off the bulb with transition=0. Firmware on this bulb
        honours transition=0 on turn_off, giving a true instant cut."""
        try:
            await self._bulb.turn_off(transition=0)
        except TypeError:
            await self._bulb.turn_off()

    async def capture_state(self) -> BulbSnapshot:
        async with self._io_lock:
            await self._ensure_connected()
            light = self._light
            hsv = getattr(light, "hsv", None)
            hue, saturation, value = _extract_hsv(hsv)
            color_temp = getattr(light, "color_temp", None) or None
            color_mode = getattr(light, "color_mode", None)
            brightness = getattr(light, "brightness", value if value > 0 else 100)
            supports_color = hasattr(light, "set_hsv")
            supports_color_temp = bool(
                hasattr(light, "set_color_temp")
                and hasattr(self._bulb, "is_variable_color_temp")
                and self._bulb.is_variable_color_temp
            )
            snap = BulbSnapshot(
                is_on=bool(self._bulb.is_on),
                brightness=_clamp(brightness, 1, 100),
                hue=hue, saturation=saturation,
                color_temp=color_temp,
                color_mode=str(color_mode) if color_mode is not None else None,
                supports_color=supports_color,
                supports_color_temp=supports_color_temp,
            )
            log.info("Captured bulb state: on=%s brightness=%s hue=%s sat=%s color=%s color_temp=%s",
                     snap.is_on, snap.brightness, snap.hue, snap.saturation,
                     snap.supports_color, snap.color_temp)
            return snap

    async def restore_state(self, snap: BulbSnapshot) -> None:
        async with self._io_lock:
            try:
                await self._ensure_connected()
                if not snap.is_on:
                    await self._bulb.turn_off()
                    return
                if snap.color_temp:
                    await self._set_color_temp_safe(snap.color_temp, snap.brightness,
                                                    self._config.restore_transition_ms)
                elif snap.supports_color:
                    await self._set_hsv_safe(snap.hue, snap.saturation, snap.brightness,
                                             self._config.restore_transition_ms)
                else:
                    await self._set_brightness_safe(snap.brightness, self._config.restore_transition_ms)
                await self._bulb.turn_on()
            except Exception:
                log.exception("Failed to restore bulb state — attempting reconnect.")
                try:
                    await self._reconnect()
                except Exception:
                    log.exception("Reconnect failed during restore.")

    async def flash_team(self, team_abbrev: str, snapshot: BulbSnapshot) -> None:
        palette = TEAM_COLORS.get(team_abbrev, DEFAULT_PALETTE)

        async with self._io_lock:
            try:
                await self._ensure_connected()
                await self._bulb.turn_on()

                loop = asyncio.get_running_loop()
                end = loop.time() + self._config.flash_duration
                next_switch = loop.time()  # absolute timestamp for next command
                use_primary = True

                while loop.time() < end:
                    h, s, v = palette.primary if use_primary else palette.secondary

                    if v == 0:
                        await self._turn_off_instant()
                    elif snapshot.supports_color:
                        await self._set_hsv_safe(h, s, _clamp(v, 1, 100),
                                                 self._config.flash_transition_ms)
                    else:
                        await self._set_brightness_safe(_clamp(v, 1, 100),
                                                        self._config.flash_transition_ms)

                    use_primary = not use_primary
                    next_switch += self._config.flash_interval
                    sleep_time = max(0.0, next_switch - loop.time())
                    await asyncio.sleep(sleep_time)

                await self._bulb.turn_on()
            except Exception:
                log.exception("Error during flash for team %s — reconnecting.", team_abbrev)
                try:
                    await self._reconnect()
                except Exception:
                    log.exception("Reconnect failed during flash.")

    async def shutdown(self) -> None:
        async with self._io_lock:
            if self._bulb is not None:
                try:
                    await self._bulb.update()
                except Exception:
                    pass


async def _safe_restore(bulb: BulbController, snapshot: BulbSnapshot) -> None:
    try:
        await bulb.restore_state(snapshot)
        log.debug("Bulb restored after quiet window.")
    except Exception:
        log.exception("Bulb restore failed.")


async def flash_worker(
    queue: asyncio.Queue[Optional[GoalEvent]],
    bulb: BulbController,
    snapshot: BulbSnapshot,
    config: AppConfig,
) -> None:
    try:
        while True:
            event = await queue.get()
            if event is None:
                queue.task_done()
                await _safe_restore(bulb, snapshot)
                return
            log.info("Flash: game=%s team=%s", event.game_id, event.scoring_team)
            await bulb.flash_team(event.scoring_team, snapshot)
            queue.task_done()

            while True:
                try:
                    next_event = queue.get_nowait()
                    if next_event is None:
                        queue.task_done()
                        await _safe_restore(bulb, snapshot)
                        return
                    log.info("Burst flash: game=%s team=%s", next_event.game_id, next_event.scoring_team)
                    await bulb.flash_team(next_event.scoring_team, snapshot)
                    queue.task_done()
                except asyncio.QueueEmpty:
                    break

            while True:
                try:
                    straggler = await asyncio.wait_for(
                        queue.get(), timeout=config.flash_quiet_window)
                    if straggler is None:
                        queue.task_done()
                        await _safe_restore(bulb, snapshot)
                        return
                    log.info("Straggler flash: game=%s team=%s", straggler.game_id, straggler.scoring_team)
                    await bulb.flash_team(straggler.scoring_team, snapshot)
                    queue.task_done()
                except asyncio.TimeoutError:
                    await _safe_restore(bulb, snapshot)
                    break
    except asyncio.CancelledError:
        await _safe_restore(bulb, snapshot)
        raise
    finally:
        await _safe_restore(bulb, snapshot)


async def monitor_game(
    game: GameInfo,
    nhl_client: NhlApiClient,
    goal_queue: asyncio.Queue[Optional[GoalEvent]],
    shutdown: asyncio.Event,
    config: AppConfig,
) -> None:
    tag = f"{game.away_abbrev}@{game.home_abbrev}"
    log.info("Tracking %s [%s]", tag, _format_local_time(game.start_time_utc))

    now = datetime.now(timezone.utc)
    wake = game.start_time_utc - timedelta(seconds=config.pregame_buffer_seconds)
    if wake > now:
        wait = (wake - now).total_seconds()
        log.info("[%s] Sleeping %d min until 5 min before puck drop.", tag, int(wait / 60))
        await _interruptible_sleep(wait, shutdown)

    last_status: Optional[GameStatus] = None
    waiting_logged = False

    while not shutdown.is_set():
        try:
            status = await nhl_client.fetch_game_status(game.game_id)
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("[%s] Failed to fetch game status.", tag)
            await _interruptible_sleep(config.poll_error_seconds, shutdown)
            continue

        if status is None:
            await _interruptible_sleep(config.poll_error_seconds, shutdown)
            continue

        if status.away_score is None or status.home_score is None:
            if status.game_state in FINAL_STATES:
                log.info("[%s] Game ended (no score payload).", tag)
                return
            if not waiting_logged and datetime.now(timezone.utc) >= game.start_time_utc:
                log.info("[%s] Waiting for score data...", tag)
                waiting_logged = True
            await _interruptible_sleep(_poll_interval(status.game_state, config), shutdown)
            continue

        a, h = status.away_score, status.home_score

        if last_status is None:
            log.info("[%s] Game started: %s %d – %s %d", tag,
                     status.away_abbrev, a, status.home_abbrev, h)
        else:
            prev_a = last_status.away_score or 0
            prev_h = last_status.home_score or 0
            away_delta = max(a - prev_a, 0)
            home_delta = max(h - prev_h, 0)

            if away_delta:
                log.info("[%s] GOAL x%d — %s! Score: %s %d – %s %d",
                         tag, away_delta, status.away_abbrev,
                         status.away_abbrev, a, status.home_abbrev, h)
                for _ in range(away_delta):
                    await _interruptible_sleep(config.goal_delay_seconds, shutdown)
                    await goal_queue.put(GoalEvent(game.game_id, status.away_abbrev))

            if home_delta:
                log.info("[%s] GOAL x%d — %s! Score: %s %d – %s %d",
                         tag, home_delta, status.home_abbrev,
                         status.away_abbrev, a, status.home_abbrev, h)
                for _ in range(home_delta):
                    await _interruptible_sleep(config.goal_delay_seconds, shutdown)
                    await goal_queue.put(GoalEvent(game.game_id, status.home_abbrev))

        last_status = status

        if status.game_state in FINAL_STATES:
            log.info("[%s] Final: %s %d – %s %d", tag,
                     status.away_abbrev, a, status.home_abbrev, h)
            return

        await _interruptible_sleep(_poll_interval(status.game_state, config), shutdown)


def select_games(games: list[GameInfo]) -> list[GameInfo]:
    if not games:
        print("No games scheduled for today.")
        return []

    print("\nToday's NHL Games")
    print("-" * 40)
    for idx, game in enumerate(games, 1):
        time_str = _format_local_time(game.start_time_utc)
        state_str = _format_schedule_state(game.game_state, time_str)
        print(f"  {idx}. {game.away_name} ({game.away_abbrev}) @ {game.home_name} ({game.home_abbrev}){state_str}")

    while True:
        raw = input("\nTrack which games? (e.g. 1,3 | 'all' | 0 to exit): ").strip().lower()
        if raw in {"0", "q", "quit", "exit"}:
            return []
        if raw == "all":
            return list(games)
        try:
            indexes = sorted({int(part.strip()) for part in raw.split(",") if part.strip()})
        except ValueError:
            print("  Invalid input — enter numbers like 1,3 or 'all'.")
            continue
        selected = [games[i - 1] for i in indexes if 1 <= i <= len(games)]
        if not selected or len(selected) != len(indexes):
            print("  One or more selections are out of range — try again.")
            continue
        return selected


def install_signal_handlers(shutdown: asyncio.Event) -> None:
    loop = asyncio.get_running_loop()
    for sig_name in ("SIGINT", "SIGTERM"):
        sig = getattr(signal, sig_name, None)
        if sig is None:
            continue
        try:
            loop.add_signal_handler(sig, shutdown.set)
        except (NotImplementedError, OSError):
            pass


async def run() -> None:
    config = build_config()

    async with NhlApiClient(config) as nhl_client:
        log.info("Fetching today's games...")
        games = await nhl_client.fetch_todays_games()

        game_ids_env = os.getenv("GAME_IDS", "").strip()
        if game_ids_env:
            id_set = set(game_ids_env.split(","))
            selected = [g for g in games if str(g.game_id) in id_set]
            if not selected:
                log.error("GAME_IDS=%r matched no games in today's schedule.", game_ids_env)
                return
            log.info("Game IDs from env: %s", ", ".join(str(g.game_id) for g in selected))
        else:
            selected = select_games(games)
            if not selected:
                print("No games selected. Exiting.")
                return

        log.info("Capturing bulb state...")
        bulb = BulbController(config)
        snapshot = await bulb.capture_state()

        shutdown = asyncio.Event()
        install_signal_handlers(shutdown)

        goal_queue: asyncio.Queue[Optional[GoalEvent]] = asyncio.Queue()
        worker_task = asyncio.create_task(flash_worker(goal_queue, bulb, snapshot, config))
        monitor_tasks = [
            asyncio.create_task(monitor_game(game, nhl_client, goal_queue, shutdown, config))
            for game in selected
        ]

        log.info("Tracking %d game(s). Press Ctrl+C to stop.", len(selected))

        try:
            await asyncio.gather(*monitor_tasks)
            await goal_queue.join()
        finally:
            shutdown.set()
            for task in monitor_tasks:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*monitor_tasks, return_exceptions=True)
            await goal_queue.put(None)
            await worker_task

        await bulb.shutdown()
        log.info("Done.")


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)-8s | %(message)s",
        datefmt="%H:%M:%S",
    )
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        logging.info("Interrupted by user.")


if __name__ == "__main__":
    main()
