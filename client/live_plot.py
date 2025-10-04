# client/live_plot.py
import time
from typing import Dict, List, Optional
import threading
import matplotlib.pyplot as plt
import matplotlib.animation as animation


def start_plot(
    state: Dict[str, List[float]],
    lock: threading.Lock,
    instruments: List[str],
    stop_event: threading.Event,
    *,
    interval_ms: int = 500,          # refresh rate
    window_points: int = 200,        # how many recent points to show (scrolling)
    auto_close: bool = False,        # False => keep window until user closes it
    run_seconds: Optional[int] = None,  # None => run forever (until close)
) -> None:
    """
    Call from the MAIN thread on macOS. Displays a live-updating, horizontally
    scrolling chart. Window closes when user closes it, or when:
      - auto_close=True and run_seconds elapse, or
      - stop_event is set (e.g., KeyboardInterrupt handler in main).
    """

    fig, ax = plt.subplots()
    lines: dict[str, any] = {}

    for sym in instruments:
        (line,) = ax.plot([], [], label=sym)
        lines[sym] = line

    ax.set_xlabel("Ticks")
    ax.set_ylabel("Price")
    ax.set_title("Live Market Data Feed (Simulated)")
    ax.legend(loc="upper left")

    t0 = time.time()

    # --- helpers -------------------------------------------------------------

    def stop_anim_and_maybe_close():
        # Stop the animation timer before closing to avoid the timer crash
        ani = getattr(fig, "_ani", None)
        if ani and getattr(ani, "event_source", None) is not None:
            ani.event_source.stop()
        if auto_close:
            try:
                plt.close(fig)
            except Exception:
                pass

    def on_close(_event):
        # User manually closed the window
        stop_event.set()
        stop_anim_and_maybe_close()

    fig.canvas.mpl_connect("close_event", on_close)

    # --- animation update loop ----------------------------------------------

    def update(_frame):
        # time-based stop (optional)
        if run_seconds is not None and (time.time() - t0) >= run_seconds:
            stop_event.set()
            stop_anim_and_maybe_close()
            return []

        if stop_event.is_set():
            stop_anim_and_maybe_close()
            return []

        updated = False
        with lock:
            for sym in instruments:
                data = state.get(sym, [])
                n = len(data)
                if n > 1:
                    # scrolling window: last `window_points` values
                    start = max(0, n - window_points)
                    x = range(start, n)
                    y = data[start:n]
                    lines[sym].set_data(x, y)
                    updated = True

        if updated:
            # set x-limits to show a moving window
            xmax = max((len(state.get(sym, [])) for sym in instruments), default=0)
            xmin = max(0, xmax - window_points)
            ax.set_xlim(xmin, max(xmin + 1, xmax))
            # autoscale y around visible data
            ax.relim()
            ax.autoscale_view(scalex=False, scaley=True)

        return list(lines.values())

    ani = animation.FuncAnimation(
        fig,
        update,
        interval=interval_ms,
        blit=False,
        cache_frame_data=False,  # avoid caching warnings
    )
    fig._ani = ani  # keep a strong reference so GC won't kill it

    # NOTE: This blocks the MAIN thread until user closes the window (or we close it)
    plt.show()
