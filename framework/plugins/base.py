from __future__ import annotations

import logging
import socket
import time
from abc import ABC
from typing import IO

logger = logging.getLogger(__name__)


class TransportPlugin(ABC):
    """
    Base class for transport / encryption plugins.

    Two transport modes
    -------------------
    1. **URL-based** (override ``sender_url`` / ``receiver_url``)
       FFmpeg handles the actual transport.  Use this for protocols that
       FFmpeg supports natively: tls://, srtp://, rtsp://, etc.

       class MyTLSPlugin(TransportPlugin):
           def sender_url(self, host, port): return f"tls://{host}:{port}"
           def receiver_url(self, port):     return f"tls://0.0.0.0:{port}?listen=1"

    2. **Pipe-based** (override ``sender_transport`` / ``receiver_transport``)
       The plugin controls the byte stream directly.  FFmpeg writes MPEG-TS
       to stdout / reads from stdin; the plugin encrypts/sends on one end
       and receives/decrypts on the other.  Use this for any custom scheme
       that FFmpeg does not natively support.

       class MyAESPlugin(TransportPlugin):
           def sender_url(self, host, port): return None   # signals pipe mode
           def receiver_url(self, port):     return None
           def sender_transport(self, stream, host, port): ...  # read, encrypt, send
           def receiver_transport(self, stream, port):     ...  # recv, decrypt, write

    Handshake
    ---------
    ``receiver_handshake`` / ``sender_handshake`` run *before* FFmpeg starts.
    Use them for key exchange, authentication, capability negotiation, etc.
    The receiver always performs its handshake first; the sender calls its
    handshake after ``startup_delay`` (or 0 s when a plugin is active).
    After both handshakes complete the sender waits ``post_handshake_delay``
    seconds to give the receiver time to start its FFmpeg listener.

    Implementing a new plugin
    -------------------------
    Drop the file in ``framework/plugins/`` and set ``plugin.name`` in
    config.yaml to the module filename (without .py).  Plugin-specific
    settings can be nested under the ``plugin`` key as well.
    """

    def __init__(self, config: dict) -> None:
        # config is the dict under the "plugin" key in config.yaml
        self.config = config

    # ------------------------------------------------------------------
    # Transport mode — URL-based (return None to switch to pipe mode)
    # ------------------------------------------------------------------

    def sender_url(self, host: str, port: int) -> str | None:
        """
        FFmpeg destination URL for the sender, or None to use pipe mode.
        Override in URL-based plugins.
        """
        return None

    def receiver_url(self, port: int) -> str | None:
        """
        FFmpeg source URL for the receiver, or None to use pipe mode.
        Override in URL-based plugins.
        """
        return None

    def sender_extra_args(self) -> list[str]:
        """Extra FFmpeg args inserted just before the output URL (URL mode only)."""
        return []

    def receiver_extra_args(self) -> list[str]:
        """Extra FFmpeg args inserted just before the output file (URL mode only)."""
        return []

    # ------------------------------------------------------------------
    # Transport mode — Pipe-based (used when sender_url returns None)
    # ------------------------------------------------------------------

    def sender_transport(self, stream: IO[bytes], host: str, port: int) -> None:
        """
        Read raw MPEG-TS from ``stream`` (FFmpeg stdout), apply custom
        encryption / framing, and send to the receiver.

        Called by sender.py when ``sender_url()`` returns None.
        This method should block until all data has been sent.
        """
        raise NotImplementedError(
            f"{type(self).__name__} returns None from sender_url() "
            "but does not implement sender_transport()"
        )

    def receiver_transport(self, stream: IO[bytes], port: int) -> None:
        """
        Receive data from the sender, apply custom decryption / deframing,
        and write the recovered MPEG-TS to ``stream`` (FFmpeg stdin).

        Called by receiver.py when ``receiver_url()`` returns None.
        This method should block until the stream ends, then return so
        the caller can close ``stream`` and let FFmpeg finish.
        """
        raise NotImplementedError(
            f"{type(self).__name__} returns None from receiver_url() "
            "but does not implement receiver_transport()"
        )

    # ------------------------------------------------------------------
    # Optional: application-level handshake (both modes)
    # ------------------------------------------------------------------

    def receiver_handshake(self, port: int) -> None:
        """
        Called on the receiver *before* FFmpeg starts.
        Block here until the handshake with the sender is complete.
        """

    def sender_handshake(self, host: str, port: int) -> None:
        """
        Called on the sender *before* FFmpeg starts.
        Block here until the handshake with the receiver is complete.
        """

    def post_handshake_delay(self) -> float:
        """
        Seconds the sender waits after ``sender_handshake()`` before
        launching FFmpeg.  Gives the receiver time to start its listener.
        """
        return 0.0

    # ------------------------------------------------------------------
    # Utility for subclasses
    # ------------------------------------------------------------------

    @staticmethod
    def _connect_with_retry(
        host: str,
        port: int,
        timeout: float = 30.0,
        interval: float = 1.0,
    ) -> socket.socket:
        """
        Return a connected TCP socket, retrying until ``timeout`` seconds
        have elapsed.  Useful in ``sender_handshake`` implementations where
        the receiver's control server may not be listening yet.

        Raises ``TimeoutError`` if the connection cannot be established
        within ``timeout`` seconds.
        """
        deadline = time.monotonic() + timeout
        last_exc: Exception = ConnectionRefusedError("never tried")
        while time.monotonic() < deadline:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            try:
                sock.connect((host, port))
                return sock
            except OSError as exc:
                sock.close()
                last_exc = exc
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    break
                logger.debug(
                    "Control port %s:%d not ready (%s), retrying in %.1fs…",
                    host, port, exc, min(interval, remaining),
                )
                time.sleep(min(interval, remaining))
        raise TimeoutError(
            f"Could not connect to {host}:{port} within {timeout}s"
        ) from last_exc
