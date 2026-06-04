#!/usr/bin/env python3
"""
mqtt_forwarder.py
─────────────────
Single-threaded daemon that reads un-sent rows from the SQLite
`mqtt_queue` table and publishes them to an MQTT broker at QOS-2.

Each message is published synchronously: the call does not return until
the broker has confirmed delivery via PUBCOMP, so no in-flight state is
needed.

Runs until SIGINT (Ctrl-C) or SIGTERM.

Dependencies
────────────
    pip install paho-mqtt
    pip install tomli        # only needed on Python < 3.11

Config
──────
    config.toml  (in the current working directory, or pass a path
                  as the first command-line argument)
"""

from __future__ import annotations

import logging
import signal
import sqlite3
import ssl
import sys
import time

# ── tomllib: stdlib on Python ≥ 3.11, else fall back to tomli ────────────────
try:
    import tomllib                        # Python ≥ 3.11
except ModuleNotFoundError:
    try:
        import tomli as tomllib           # type: ignore[no-redef]
    except ModuleNotFoundError:
        # Logging not yet configured; Python's last-resort handler writes to stderr.
        logging.error(
            "tomllib not available. "
            "Python ≥ 3.11 includes it in the stdlib. "
            "For older Python: pip install tomli"
        )
        sys.exit(1)

import paho.mqtt.client as mqtt

# ── detect paho v2 (CallbackAPIVersion was introduced in 2.0.0) ───────────────
try:
    from paho.mqtt.client import CallbackAPIVersion as _CbApi
    _PAHO_V2 = True
except ImportError:
    _PAHO_V2 = False


# ─────────────────────────────────────────────────────────────────────────────
# Logging setup
# ─────────────────────────────────────────────────────────────────────────────

TRACE = 5                              # one level below DEBUG
logging.addLevelName(TRACE, "TRACE")

_LOG_LEVELS: dict[str, int] = {
    "trace":   TRACE,
    "debug":   logging.DEBUG,
    "info":    logging.INFO,
    "warn":    logging.WARNING,
    "warning": logging.WARNING,
    "error":   logging.ERROR,
}

log = logging.getLogger("mqtt_fwd")


def _setup_logging(level_name: str) -> None:
    level = _LOG_LEVELS.get(level_name.lower(), logging.INFO)
    logging.basicConfig(
        level=level,
        format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
        stream=sys.stdout,
        force=True,     # reconfigure if basicConfig was called earlier (Python ≥ 3.8)
    )
    log.setLevel(level)


# ─────────────────────────────────────────────────────────────────────────────
# Shared mutable state
# (single-threaded → no locks; callbacks fire inside client.loop() calls)
# ─────────────────────────────────────────────────────────────────────────────

_connected: bool = False
_running:   bool = True

# ── tunables ──────────────────────────────────────────────────────────────────
LOOP_TIMEOUT_S   = 0.05    # paho loop() socket-wait ceiling per call
IDLE_SLEEP_S     = 0.10    # sleep when the SQLite queue is empty
PUBLISH_TIMEOUT_S = 14.0   # wait up to 14 s for PUBCOMP (also used as drain budget on shutdown)
MAX_RECONNECT_S  = 60      # cap on exponential back-off


# ─────────────────────────────────────────────────────────────────────────────
# Signal handling
# ─────────────────────────────────────────────────────────────────────────────

def _handle_signal(signum: int, _frame: object) -> None:
    global _running
    try:
        name = signal.Signals(signum).name
    except ValueError:
        name = str(signum)
    log.info("Signal %s received – shutting down gracefully.", name)
    _running = False


signal.signal(signal.SIGINT,  _handle_signal)
signal.signal(signal.SIGTERM, _handle_signal)


# ─────────────────────────────────────────────────────────────────────────────
# MQTT callbacks  (two flavours: paho v2 and v1)
# ─────────────────────────────────────────────────────────────────────────────

if _PAHO_V2:
    def _on_connect(client, userdata, connect_flags, reason_code, properties):   # noqa: ANN001
        global _connected
        if reason_code.is_failure:
            _connected = False
            log.warning("MQTT connect refused: %s", reason_code)
        else:
            _connected = True
            log.info("Connected to MQTT broker.")

    def _on_disconnect(client, userdata, disconnect_flags, reason_code, properties):  # noqa: ANN001
        global _connected
        _connected = False
        code = getattr(reason_code, "value", reason_code)
        if code == 0:
            log.info("Disconnected cleanly from broker.")
        else:
            log.warning("Unexpected disconnect: %s", reason_code)

else:  # paho v1 ─────────────────────────────────────────────────────────────
    def _on_connect(client, userdata, flags, rc):     # type: ignore[misc]  # noqa: ANN001
        global _connected
        if rc == 0:
            _connected = True
            log.info("Connected to MQTT broker.")
        else:
            _connected = False
            log.warning("MQTT connect refused (rc=%d).", rc)

    def _on_disconnect(client, userdata, rc):         # type: ignore[misc]  # noqa: ANN001
        global _connected
        _connected = False
        if rc == 0:
            log.info("Disconnected cleanly from broker.")
        else:
            log.warning("Unexpected disconnect (rc=%d).", rc)


# ── paho internal log → our logger ───────────────────────────────────────────
# Same callback signature for both paho v1 and v2.
# This is the only reliable way to surface SSL/TLS handshake errors (e.g.
# WRONG_VERSION_NUMBER when connecting without TLS to a TLS-only broker, or
# certificate verification failures) because paho catches ssl.SSLError
# internally and reports it exclusively through this channel.

_PAHO_TO_LOGGING: dict[int, int] = {
    mqtt.MQTT_LOG_DEBUG:   TRACE,
    mqtt.MQTT_LOG_INFO:    logging.DEBUG,
    mqtt.MQTT_LOG_NOTICE:  logging.DEBUG,
    mqtt.MQTT_LOG_WARNING: logging.WARNING,
    mqtt.MQTT_LOG_ERR:     logging.ERROR,
}


def _on_paho_log(client, userdata, level, buf: str) -> None:   # noqa: ANN001
    our_level = _PAHO_TO_LOGGING.get(level, logging.DEBUG)
    log.log(our_level, "paho: %s", buf)


# ─────────────────────────────────────────────────────────────────────────────
# SQLite
# ─────────────────────────────────────────────────────────────────────────────

def _open_db(path: str) -> sqlite3.Connection:
    db = sqlite3.connect(path, timeout=14, check_same_thread=True)
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA journal_mode=WAL")
    db.execute("PRAGMA busy_timeout=14000")
    log.info("Opened SQLite '%s' in WAL mode.", path)
    return db


def _fetch_next_unsent(db: sqlite3.Connection) -> sqlite3.Row | None:
    """Return the single oldest un-sent row (by timestamp), or None if the queue is empty."""
    return db.execute(
        "SELECT id, ts, topic, message, retain "
        "FROM   mqtt_queue "
        "WHERE  sent = 0 "
        "ORDER  BY ts ASC "
        "LIMIT  1",
    ).fetchone()


# ─────────────────────────────────────────────────────────────────────────────
# TLS hostname override
# ─────────────────────────────────────────────────────────────────────────────

class _SSLContextProxy:
    """Transparent proxy for ssl.SSLContext that overrides server_hostname.

    When a broker is reached via NAT or port-forwarding (e.g. connecting to
    192.168.10.1:8883 but the real server is mqtt.server.com), paho would
    normally pass the connection IP as server_hostname to wrap_socket(), so
    TLS verification and SNI both use the IP rather than the broker's name —
    causing cert verification to fail.

    This proxy intercepts wrap_socket() and injects the canonical hostname
    instead.  All other attribute access (verify_mode, check_hostname, …) is
    forwarded transparently to the real SSLContext, so paho and tls_insecure_set()
    work without modification.

    Note: client._ssl_context is a semi-private paho attribute that has been
    stable since paho 1.3.  It is the only supported injection point short of
    monkey-patching paho internals.
    """

    def __init__(self, context: ssl.SSLContext, server_hostname: str) -> None:
        # Use object.__setattr__ to avoid hitting our own __setattr__.
        object.__setattr__(self, "_ctx", context)
        object.__setattr__(self, "_server_hostname", server_hostname)

    def wrap_socket(self, sock, *args, **kwargs):   # noqa: ANN001
        hostname = object.__getattribute__(self, "_server_hostname")
        kwargs["server_hostname"] = hostname
        return object.__getattribute__(self, "_ctx").wrap_socket(sock, *args, **kwargs)

    def __getattr__(self, name: str):               # noqa: ANN204
        return getattr(object.__getattribute__(self, "_ctx"), name)

    def __setattr__(self, name: str, value) -> None:  # noqa: ANN001
        # Forward attribute sets (e.g. check_hostname, verify_mode set by
        # paho's tls_insecure_set()) to the real context.
        setattr(object.__getattribute__(self, "_ctx"), name, value)


# ─────────────────────────────────────────────────────────────────────────────
# MQTT client factory
# ─────────────────────────────────────────────────────────────────────────────

def _build_client(cfg: dict) -> mqtt.Client:
    mcfg = cfg.get("mqtt", {})

    kwargs: dict = dict(
        client_id     = "",            # let the broker assign one
        clean_session = True,
        userdata      = None,
        protocol      = mqtt.MQTTv311,
    )
    if _PAHO_V2:
        kwargs["callback_api_version"] = _CbApi.VERSION2

    client = mqtt.Client(**kwargs)
    client.on_connect    = _on_connect
    client.on_disconnect = _on_disconnect
    client.on_log        = _on_paho_log

    username = mcfg.get("username", "")
    password = mcfg.get("password", "")
    if username:
        client.username_pw_set(username, password or None)

    certfile = mcfg.get("certfile", "").strip() or None
    keyfile  = mcfg.get("keyfile",  "").strip() or None
    if bool(certfile) != bool(keyfile):
        log.error(
            "Config error: 'certfile' and 'keyfile' must both be set or both be empty."
        )
        sys.exit(1)
    if (certfile or keyfile) and not mcfg.get("tls", False):
        log.error(
            "Config error: 'certfile'/'keyfile' require 'tls = true'."
        )
        sys.exit(1)

    if mcfg.get("tls", False):
        ca_file          = mcfg.get("ca_file", "").strip() or None
        verify           = bool(mcfg.get("verify_ca", True))
        tls_hostname     = mcfg.get("tls_hostname", "").strip() or None
        keyfile_password = mcfg.get("keyfile_password", "").strip() or None

        client.tls_set(
            ca_certs         = ca_file,           # None → use system CA store
            certfile         = certfile,
            keyfile          = keyfile,
            keyfile_password = keyfile_password,
            cert_reqs        = ssl.CERT_REQUIRED if verify else ssl.CERT_NONE,
        )
        if not verify:
            client.tls_insecure_set(True)         # also disables hostname check

        if tls_hostname and verify:
            # Connection goes to a NAT/port-forwarded address; verify the cert
            # against the broker's real hostname instead of the connection host.
            client._ssl_context = _SSLContextProxy(client._ssl_context, tls_hostname)
            log.info(
                "TLS hostname override: cert will be verified as '%s'.",
                tls_hostname,
            )

        log.info(
            "TLS enabled  verify_ca=%s  ca_file=%s  tls_hostname=%s  "
            "client_cert=%s",
            verify,
            ca_file or "<system CAs>",
            tls_hostname or "<same as host>",
            certfile or "<none>",
        )

    return client


# ─────────────────────────────────────────────────────────────────────────────
# Synchronous QOS-2 publish
# ─────────────────────────────────────────────────────────────────────────────

def _publish_sync(
    client: mqtt.Client,
    topic: str,
    payload: str,
    retain: bool,
) -> bool:
    """Publish a QOS-2 message and block until PUBCOMP is received.

    Drives the paho network loop internally so no external loop() call is
    needed while waiting.  Returns True on confirmed delivery, False if the
    connection dropped, the forwarder is stopping, or a timeout occurred.
    """
    result = client.publish(topic, payload, qos=2, retain=retain)

    if result.rc != mqtt.MQTT_ERR_SUCCESS:
        log.warning("publish() rejected immediately (rc=%d).", result.rc)
        return False

    deadline = time.monotonic() + PUBLISH_TIMEOUT_S

    while not result.is_published():
        if time.monotonic() > deadline:
            log.warning(
                "QOS-2 PUBCOMP not received within %.0fs – "
                "treating as failed.",
                PUBLISH_TIMEOUT_S,
            )
            return False

        rc = client.loop(timeout=LOOP_TIMEOUT_S)
        if rc not in (mqtt.MQTT_ERR_SUCCESS, mqtt.MQTT_ERR_NO_CONN):
            log.warning("loop() rc=%d during publish wait.", rc)
            return False

        if not _connected:
            log.warning("Disconnected while waiting for PUBCOMP.")
            return False

    return True


# ─────────────────────────────────────────────────────────────────────────────
# Main loop
# ─────────────────────────────────────────────────────────────────────────────

def main() -> None:
    global _connected, _running

    config_path = sys.argv[1] if len(sys.argv) > 1 else "config.toml"

    try:
        with open(config_path, "rb") as fh:
            cfg = tomllib.load(fh)
    except FileNotFoundError:
        logging.error("Config file not found: %s", config_path)
        sys.exit(1)
    except Exception as exc:
        logging.error("Error reading %s: %s", config_path, exc)
        sys.exit(1)

    _setup_logging(cfg.get("log", {}).get("level", "info"))

    mcfg = cfg.get("mqtt", {})
    host = mcfg.get("host", "localhost")
    port = int(mcfg.get("port", 1883))

    db_path = cfg.get("sqlite", {}).get("file", "queue.db")

    db     = _open_db(db_path)
    client = _build_client(cfg)

    reconnect_delay = 1.0
    last_attempt    = -9_999.0   # trigger an immediate first connect attempt

    log.info("mqtt_forwarder started  host=%s  port=%d", host, port)

    while _running:

        # ── (re)connect when not connected ───────────────────────────────────
        if not _connected:
            now       = time.monotonic()
            wait_left = (last_attempt + reconnect_delay) - now

            if wait_left > 0:
                # Still in the back-off window; drive socket so callbacks fire.
                client.loop(timeout=min(wait_left, LOOP_TIMEOUT_S))
                time.sleep(0.02)
                continue

            log.info(
                "Connecting to %s:%d …  (back-off %ds)",
                host, port, int(reconnect_delay),
            )
            try:
                # Always call connect() so the full TCP+MQTT handshake is
                # redone cleanly each time.
                client.connect(host, port, keepalive=60)
            except OSError as exc:
                log.warning(
                    "Connect failed: %s.  Retry in %ds.", exc, int(reconnect_delay)
                )
                last_attempt    = time.monotonic()
                reconnect_delay = min(reconnect_delay * 2, MAX_RECONNECT_S)
                continue

            last_attempt = time.monotonic()
            # Give paho time to complete the CONNACK handshake.
            client.loop(timeout=0.5)
            if _connected:
                reconnect_delay = 1.0   # reset back-off on success
            else:
                # connect() didn't raise but the MQTT/TLS handshake failed
                # (on_disconnect or on_log will have logged the detail).
                # Back off just as we would for a refused TCP connection.
                reconnect_delay = min(reconnect_delay * 2, MAX_RECONNECT_S)
                log.warning("Handshake failed – retry in %ds.", int(reconnect_delay))
            continue

        # ── drive MQTT network I/O ────────────────────────────────────────────
        rc = client.loop(timeout=LOOP_TIMEOUT_S)
        if rc not in (mqtt.MQTT_ERR_SUCCESS, mqtt.MQTT_ERR_NO_CONN):
            log.warning("paho loop() returned rc=%d – reconnecting.", rc)
            _connected = False
            continue

        if not _connected:
            # on_disconnect() fired inside loop() above; go back to top.
            continue

        # ── fetch and publish the next un-sent row ────────────────────────────
        row = _fetch_next_unsent(db)
        if row is None:
            time.sleep(IDLE_SLEEP_S)
            continue

        log.debug(
            "Publishing  row=%-6d  topic=%s",
            row["id"], row["topic"],
        )
        log.log(TRACE,
            "Publishing  row=%-6d  topic=%s  message=%s",
            row["id"], row["topic"], row["message"],
        )

        ok = _publish_sync(
            client,
            topic   = row["topic"],
            payload = row["message"],
            retain  = bool(row["retain"]),
        )

        if ok:
            try:
                db.execute("UPDATE mqtt_queue SET sent=1 WHERE id=?", (row["id"],))
                db.commit()
                log.debug("Row %d marked sent.", row["id"])
            except sqlite3.Error as exc:
                log.error("DB error marking row %d sent: %s", row["id"], exc)
        else:
            log.warning(
                "Publish failed for row %d – will retry after reconnect.", row["id"]
            )

    # ── graceful shutdown ─────────────────────────────────────────────────────
    if _connected:
        client.disconnect()
        client.loop(timeout=1.0)

    db.close()
    log.info("mqtt_forwarder stopped.")


if __name__ == "__main__":
    main()