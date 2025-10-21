from flask import Flask, request, jsonify
from storage import save_to_csv
from werkzeug.serving import make_server
import threading


def create_app():
    app = Flask(__name__)

    @app.route("/collect", methods=["POST"])
    def collect():
        data = request.get_json(force=True)
        # Expecting keys as in save_to_csv
        save_to_csv(data)
        return jsonify({"status": "ok"}), 200

    return app


class HttpCollectorServer:
    def __init__(self, app: Flask, host="127.0.0.1", port=5000):
        self.app = app
        self.host = host
        self.port = port
        self._server = None
        self._thread = None

    def start(self):
        if self._thread and self._thread.is_alive():
            return
        self._server = make_server(self.host, self.port, self.app)
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)
        self._thread.start()

    def stop(self):
        if self._server:
            self._server.shutdown()
        if self._thread:
            self._thread.join(timeout=2)
