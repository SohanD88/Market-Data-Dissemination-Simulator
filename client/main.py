# client/main.py
import argparse
import grpc

from md import marketdata_pb2 as pb2
from md import marketdata_pb2_grpc as pb2_grpc


def do_ping(stub):
    reply = stub.Ping(pb2.PingRequest(message="hello, server"))
    print(f"[client] ping reply: {reply.message}")


def do_stream(stub, instrument: str, max_msgs: int | None):
    req = pb2.SubscribeRequest(instrument_id=instrument)
    print(f"[client] streaming {instrument} (max_msgs={max_msgs or '∞'}) ...")
    count = 0
    for up in stub.StreamPrices(req):
        print(f"[client] {up.instrument_id} | {up.ts_ms} | {up.value}")
        count += 1
        if max_msgs and count >= max_msgs:
            print("[client] reached max messages, exiting stream.")
            break


def main(host: str, port: int, instrument: str, max_msgs: int | None, stream: bool):
    addr = f"{host}:{port}"
    with grpc.insecure_channel(addr) as channel:
        stub = pb2_grpc.MarketDataStub(channel)
        do_ping(stub)
        if stream:
            do_stream(stub, instrument, max_msgs)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=50051)
    ap.add_argument("--instrument", default="ES")
    ap.add_argument("--max-msgs", type=int, default=10, help="0 = infinite")
    ap.add_argument("--stream", action="store_true", help="also run the live stream demo")
    args = ap.parse_args()
    max_msgs = None if args.max_msgs == 0 else args.max_msgs
    main(args.host, args.port, args.instrument, max_msgs, args.stream)


