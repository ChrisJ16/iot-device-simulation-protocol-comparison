import threading
import asyncio
import subprocess
import shutil
import time


class LocalBroker:
    """Start a local MQTT broker.

    Primary method: use hbmqtt.Broker (runs in an asyncio event loop).
    Fallback: try to start system 'mosquitto' broker process if available.

    This module avoids importing hbmqtt at top-level to prevent ImportError
    from incompatible dependencies (e.g. websockets versions).
    """

    def __init__(self, host='127.0.0.1', port=1883):
        self.host = host
        self.port = port
        self._mode = None  # 'hbmqtt' or 'mosquitto' or None
        self._thread = None
        self._loop = None
        self._broker = None
        self._mosquitto_proc = None

    def _try_start_hbmqtt(self):
        try:
            from hbmqtt.broker import Broker
        except Exception as e:
            # Could be ImportError or runtime incompatibilities (websockets, etc.)
            return False, f"hbmqtt import failed: {e}"

        config = {
            'listeners': {
                'default': {
                    'type': 'tcp',
                    'bind': f'{self.host}:{self.port}'
                }
            },
            'sys_interval': 10,
            'topic-check': {
                'enabled': False
            }
        }

        self._broker = Broker(config)

        def _run():
            self._loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self._loop)
            try:
                self._loop.run_until_complete(self._broker.start())
                self._loop.run_forever()
            finally:
                try:
                    self._loop.run_until_complete(self._broker.shutdown())
                except Exception:
                    pass

        self._thread = threading.Thread(target=_run, daemon=True)
        self._thread.start()
        # Give it some time to start
        time.sleep(0.2)
        return True, "hbmqtt started"

    def _try_start_mosquitto(self):
        mosq = shutil.which('mosquitto')
        if not mosq:
            return False, 'mosquitto not found'
        # Start mosquitto in background, bind to requested port if possible
        try:
            # -p sets the port
            proc = subprocess.Popen([mosq, '-p', str(self.port)], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            self._mosquitto_proc = proc
            # give it a moment to start
            time.sleep(0.2)
            return True, 'mosquitto started'
        except Exception as e:
            return False, f'mosquitto start failed: {e}'

    def start(self):
        # Try hbmqtt first (lazy import)
        ok, msg = self._try_start_hbmqtt()
        if ok:
            self._mode = 'hbmqtt'
            return

        # hbmqtt failed; try mosquitto
        ok2, msg2 = self._try_start_mosquitto()
        if ok2:
            self._mode = 'mosquitto'
            return

        # If both failed, raise with helpful message
        raise RuntimeError(f"Failed to start local MQTT broker: {msg}; {msg2}")

    def stop(self):
        if self._mode == 'hbmqtt' and self._broker and self._loop:
            try:
                self._loop.call_soon_threadsafe(self._loop.stop)
            except Exception:
                pass
            if self._thread:
                self._thread.join(timeout=2)
        elif self._mode == 'mosquitto' and self._mosquitto_proc:
            try:
                self._mosquitto_proc.terminate()
                self._mosquitto_proc.wait(timeout=2)
            except Exception:
                pass
        self._mode = None
