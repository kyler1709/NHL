# NHL Goal Light 🚨

Real-time NHL goal detector that flashes your **TP-Link Kasa smart bulb(s)** in your favourite team's colours the moment a goal is scored — with a web-based GUI to configure everything.

![Python](https://img.shields.io/badge/Python-3.9%2B-blue?style=flat-square)
![Flask](https://img.shields.io/badge/Flask-SSE-lightgrey?style=flat-square)
![License](https://img.shields.io/badge/License-MIT-green?style=flat-square)

---

## Features

- 🏒 **Live goal detection** — polls the NHL API every ~1 s during live play, drops to 0.75 s in the final minute of a period
- 💡 **Multi-bulb support** — flash multiple Kasa bulbs simultaneously; add one IP per line in the GUI
- 🎨 **Team colour flashing** — bulb strobes between each team's primary and secondary HSV colours
- 📺 **Per-game TV delay** — set a custom stream delay (seconds) per game row so the flash syncs perfectly with your broadcast
- 🌐 **Web GUI** — dark/light theme, live log stream, sidebar settings, mobile responsive with slide-in drawer
- ⚡ **Simulate Goal** — fire a test flash for any team without waiting for a real goal
- 🔄 **Exponential backoff** — graceful retry logic on NHL API failures
- 💾 **Persistent settings** — bulb IPs saved to `localStorage` across sessions

---

## Requirements

- Python 3.9+
- TP-Link Kasa smart bulb (colour, e.g. KL130)
- Bulb on the same local network as the machine running the script

```bash
pip install flask python-kasa requests
```

---

## Quick Start

```bash
# 1. Clone
git clone https://github.com/kyler1709/NHL-BULB.git
cd NHL-BULB

# 2. Install dependencies
pip install flask python-kasa requests

# 3. Run the server
python nhl_goal_light_server.py

# 4. Open the GUI
# Visit http://localhost:5000 in your browser (or your LAN IP for mobile)
```

---

## GUI Overview

### Sidebar — Settings

| Section | Key Settings |
|---|---|
| **Bulbs** | One IP per line — all bulbs flash simultaneously |
| **Flash** | Duration, strobe interval, crossfade transition |
| **Polling** | Live / Critical / Pregame / Error intervals |
| **Network** | Request timeout, max retries, backoff base & max |
| **Timing** | Pregame buffer, restore transition, default TV delay |

### Main Panel

- **Today's Games** — lists all NHL games with live/final/scheduled badges, team colour swatches, and a per-game TV delay override
- **Live Log** — SSE-streamed output; goals highlighted in red, errors in orange
- **Simulate Goal** — pick any team and fire a test flash instantly

## Configuration Reference

| Parameter | Default | Description |
|---|---|---|
| `flash_duration` | 12 s | How long the bulb flashes after a goal |
| `flash_interval` | 0.45 s | Strobe speed (lower = faster) |
| `flash_quiet_window` | 1.5 s | Post-flash wait before restoring (burst goal protection) |
| `flash_transition_ms` | 120 ms | Colour crossfade time (0 = instant) |
| `poll_live_seconds` | 1.0 s | API poll rate during live play |
| `poll_critical_seconds` | 0.75 s | Poll rate in the final minute of a period |
| `poll_pregame_seconds` | 10 s | Poll rate before puck drop |
| `poll_error_seconds` | 5 s | Retry wait after API failure |
| `request_timeout` | 10 s | NHL API request timeout |
| `max_retries` | 4 | Max retries per failed request |
| `backoff_base` | 1.0 s | Starting backoff (doubles each retry) |
| `backoff_max` | 30 s | Backoff cap |
| `pregame_buffer_seconds` | 300 s | Wake-up lead time before puck drop |
| `restore_transition_ms` | 150 ms | Fade time back to original colour |
| `goal_delay_seconds` | 35 s | Global TV delay (overridable per game) |

---

## License

MIT
