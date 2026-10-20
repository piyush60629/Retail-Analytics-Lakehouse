"""Record a short GIF tour of the Streamlit dashboard (no API keys needed).

    streamlit run dashboard/app.py --server.headless true &
    python scripts/record_dashboard.py --out docs/dashboard.gif

Needs: pip install playwright && playwright install chromium, and ffmpeg.
"""

from __future__ import annotations

import argparse
import subprocess
import tempfile
import time
from pathlib import Path

from playwright.sync_api import Page, sync_playwright

VIEWPORT = {"width": 1280, "height": 860}
TABS = ["Pipeline", "Validation", "CDC & SCD Type 2", "Data quality"]


def pause(page: Page, seconds: float) -> None:
    page.wait_for_timeout(int(seconds * 1000))


def scroll(page: Page, pixels: int, steps: int = 10) -> None:
    for _ in range(steps):
        page.mouse.wheel(0, pixels / steps)
        pause(page, 0.15)


def record(url: str, video_dir: Path) -> tuple[Path, float]:
    """Returns the video and the number of seconds to cut from its start (page loading)."""

    with sync_playwright() as p:
        browser = p.chromium.launch()
        context = browser.new_context(viewport=VIEWPORT, record_video_dir=str(video_dir), record_video_size=VIEWPORT)
        page = context.new_page()
        started = time.monotonic()
        page.goto(url, wait_until="domcontentloaded", timeout=120_000)
        page.get_by_role("tab", name="Business KPIs").wait_for(timeout=120_000)
        page.locator(".js-plotly-plot").first.wait_for(timeout=60_000)
        pause(page, 1)
        ready = time.monotonic() - started
        page.mouse.move(VIEWPORT["width"] * 0.6, VIEWPORT["height"] * 0.6)
        pause(page, 2.5)
        scroll(page, 900)
        pause(page, 2)
        scroll(page, 900)
        pause(page, 2)
        scroll(page, -2400)
        for name in TABS:
            page.get_by_role("tab", name=name).click()
            pause(page, 3)
            scroll(page, 600)
            pause(page, 1.5)
            scroll(page, -1200)
        video = Path(page.video.path())
        context.close()
        browser.close()
        return video, max(0.0, ready - 0.5)


def to_gif(video: Path, out: Path, speed: float, width: int, fps: int, trim: float = 0.0) -> None:
    out.parent.mkdir(parents=True, exist_ok=True)
    filters = f"setpts=PTS/{speed},fps={fps},scale={width}:-1:flags=lanczos"
    with tempfile.TemporaryDirectory() as tmp:
        palette = Path(tmp) / "palette.png"
        subprocess.run(["ffmpeg", "-y", "-loglevel", "error", "-ss", f"{trim:.2f}", "-i", str(video),
                        "-vf", f"{filters},palettegen=max_colors=96:stats_mode=diff", str(palette)], check=True)
        subprocess.run(["ffmpeg", "-y", "-loglevel", "error", "-ss", f"{trim:.2f}", "-i", str(video), "-i", str(palette),
                        "-lavfi", f"{filters} [x]; [x][1:v] paletteuse=dither=bayer:bayer_scale=4:diff_mode=rectangle",
                        str(out)], check=True)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--url", default="http://localhost:8501")
    parser.add_argument("--out", type=Path, default=Path("docs/dashboard.gif"))
    parser.add_argument("--speed", type=float, default=2.0)
    parser.add_argument("--width", type=int, default=880)
    parser.add_argument("--fps", type=int, default=6)
    args = parser.parse_args()
    with tempfile.TemporaryDirectory() as tmp:
        video, trim = record(args.url, Path(tmp))
        to_gif(video, args.out, args.speed, args.width, args.fps, trim)
    print(f"Wrote {args.out} ({args.out.stat().st_size / 1_000_000:.1f} MB)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
