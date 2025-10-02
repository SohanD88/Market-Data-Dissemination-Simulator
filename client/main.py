# client/main.py
import argparse
import threading
import time
from typing import Dict, List, Optional
import traceback
import grpc

from md import marketdata_pb2 as pb2
from md import marketdata_pb2_grpc as pb2_grpc


# ---- helpers ---------------------------------------------------------------

def do_ping(stub: pb2_grpc.MarketDataStub) -> None:
    """Quick connectivity check."""
    reply = stub.Ping(pb2.PingRequest(message="hello, server"))
    print(f"[client] ping reply: {reply.message}")


def stream_worker(
    stub: pb2_grpc.MarketDataStub,
    instrument: str,
    max_msgs: Optional[int],
    state: Dict[str, List[float]],
    lock: threading.Lock,
    stop_event: threading.Event,
    keep_last: int = 5,
) -> None:
    """
    Subscribe to one instrument and keep its recent values in `state[instrument]`.
    Handles Day-5 messages:
      - Update{ snapshot { prices: [...] } }
      - Update{ incremental { value: x } }
    """
    req = pb2.SubscribeRequest(instrument_id=instrument)
    count = 0

    try:
        call = stub.StreamPrices(req)
        for up in call:
            if stop_event.is_set():
                # Ask gRPC to tear down this stream (will raise CANCELLED).
                try:
                    call.cancel()  # type: ignore[attr-defined]
                except Exception:
                    pass
                break

            # --- Snapshot ---
            if up.HasField("snapshot"):
                prices = list(up.snapshot.prices)
                with lock:
                    state[instrument] = prices[-keep_last:] if keep_last else prices
                print(f"[client][{instrument}] SNAPSHOT {state[instrument]}")

            # --- Incremental ---
            elif up.HasField("incremental"):
                with lock:
                    arr = state.get(instrument, [])
                    arr.append(up.incremental.value)
                    if keep_last and len(arr) > keep_last:
                        arr = arr[-keep_last:]
                    state[instrument] = arr

            count += 1
            if max_msgs and count >= max_msgs:
                break

    except grpc.RpcError as e:
        # CANCELLED is expected if we shut the stream down intentionally.
        code = e.code() if hasattr(e, "code") else None
        if code != grpc.StatusCode.CANCELLED:
            print(f"[client][{instrument}] gRPC error: {e} (code={code})")
            traceback.print_exc()
    except Exception:
        print(f"[client][{instrument}] unexpected error:")
        traceback.print_exc()


def print_state_periodically(
    state: Dict[str, List[float]],
    lock: threading.Lock,
    instruments: List[str],
    interval: float,
    run_seconds: Optional[int],
    stop_event: threading.Event,
) -> None:
    """
    Every `interval` seconds, print a small table of the latest values
    for each instrument. Stops after `run_seconds` (if provided) or when
    `stop_event` is set.
    """
    t0 = time.time()
    while not stop_event.is_set():
        with lock:
            lines: List[str] = []
            for sym in instruments:
                latest_list = state.get(sym, [])
                if latest_list:
                    lines.append(f"{sym:>6}: {latest_list[-1]:.4f}")
                else:
                    lines.append(f"{sym:>6}: …")  # waiting for first update

        print(f"\n[client] {time.strftime('%X')} latest")
        print("\n".join(lines))

        time.sleep(interval)
        if run_seconds and (time.time() - t0) >= run_seconds:
            break

    stop_event.set()  # idempotent


# ---- main ------------------------------------------------------------------

def main(
    host: str,
    port: int,
    instruments: List[str],
    max_msgs: Optional[int],
    run_seconds: Optional[int],
) -> None:
    addr = f"{host}:{port}"
    state: Dict[str, List[float]] = {}  # instrument -> recent values
    lock = threading.Lock()
    stop_event = threading.Event()

    with grpc.insecure_channel(addr) as channel:
        stub = pb2_grpc.MarketDataStub(channel)
        do_ping(stub)

        # start one worker per instrument (non-daemon so we can join)
        threads: List[threading.Thread] = []
        for sym in instruments:
            th = threading.Thread(
                target=stream_worker,
                args=(stub, sym, max_msgs, state, lock, stop_event),
                daemon=False,
            )
            th.start()
            threads.append(th)

        # print table periodically for N seconds (or until stop_event)
        print_state_periodically(
            state, lock, instruments,
            interval=1.0,
            run_seconds=run_seconds,
            stop_event=stop_event,
        )

        # tell workers to stop and wait for them
        stop_event.set()
        for th in threads:
            th.join()


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=50051)
    ap.add_argument(
        "--instruments",
        default="ES,NQ",
        help="Comma-separated list, e.g. ES,NQ,CL",
    )
    ap.add_argument("--max-msgs", type=int, default=0, help="0 = infinite per instrument")
    ap.add_argument("--seconds", type=int, default=8, help="0 = run until streams end")
    args = ap.parse_args()

    instruments: List[str] = [s.strip() for s in args.instruments.split(",") if s.strip()]
    max_msgs: Optional[int] = None if args.max_msgs == 0 else args.max_msgs
    run_seconds: Optional[int] = None if args.seconds == 0 else args.seconds

    main(args.host, args.port, instruments, max_msgs, run_seconds)
