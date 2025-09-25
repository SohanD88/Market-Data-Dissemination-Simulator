# server/main.py
from concurrent import futures
import time
import random
import grpc

from md import marketdata_pb2 as pb2
from md import marketdata_pb2_grpc as pb2_grpc


class MarketDataServicer(pb2_grpc.MarketDataServicer):
    # --- Day 2: simple unary RPC ---
    def Ping(self, request, context):
        print(f"[server] Ping received: {request.message!r}")
        return pb2.PingReply(message=f"pong: {request.message}")

    # --- Day 3: basic server->client streaming (optional) ---
    def StreamPrices(self, request, context):
        instrument = request.instrument_id or "ES"
        print(f"[server] StreamPrices started for {instrument}")
        value = random.uniform(100.0, 200.0)
        while context.is_active():
            value = max(0.0, value + random.uniform(-0.5, 0.5))  # tiny random walk
            yield pb2.Update(
                instrument_id=instrument,
                ts_ms=int(time.time() * 1000),
                value=float(f"{value:.4f}"),
            )
            time.sleep(0.5)
        print(f"[server] StreamPrices ended for {instrument}")


def serve(host: str = "127.0.0.1", port: int = 50051):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=8))
    pb2_grpc.add_MarketDataServicer_to_server(MarketDataServicer(), server)
    addr = f"{host}:{port}"
    server.add_insecure_port(addr)
    server.start()
    print(f"[server] running on {addr}")
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        print("\n[server] shutting down...")


if __name__ == "__main__":
    serve()
