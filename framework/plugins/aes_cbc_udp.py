"""
AES-128-CBC over UDP with simple RSA-PKCS1v15 key exchange.

Key exchange (RSA-PKCS1v15)
---------------------------
The AES session key is wrapped with a textbook-style RSA public-key
encryption (PKCS#1 v1.5 padding) over a short-lived TCP control
channel. The data stream itself runs over UDP.

1. Receiver generates a fresh RSA-2048 keypair.
2. Receiver listens on the control port (data_port + 1, TCP) and sends
   the *public* key to the sender once it connects.
3. Sender generates a random 16-byte AES-128 key.
4. Sender encrypts that key with the receiver's public key
   (RSA / PKCS#1 v1.5) and sends the ciphertext back.
5. Receiver decrypts with its private key.
6. Receiver acknowledges, both sides drop the TCP control connection,
   and the AES session key now exists on both ends -- never on the wire
   in the clear.

  Receiver                            Sender
    |                                    |
    | generate RSA-2048 keypair          |
    | listen on ctrl port (port + 1) TCP |
    |                                    | sender_handshake()
    |<-------- TCP connect ------------- |
    |--- len(4B) + public key (PEM) ---->|
    |<-- len(4B) + PKCS1v15 ciphertext --|  wraps 16-byte AES-128 key
    |--- b"ACK" ----------------------> |
    |                                    |
    | private_key.decrypt() -> AES key   | sender already has key
    | bind UDP data socket (port)        | sleep post_handshake_delay (1 s)
    |                                    |
    | start FFmpeg  stdin=PIPE           | start FFmpeg  stdout=PIPE
    |<==== AES-128-CBC datagrams (UDP) ==| sender_transport()
    | receiver_transport() decrypts      |
    | writes plaintext to FFmpeg stdin   |

Datagram layout (independently decryptable, one UDP packet each)
----------------------------------------------------------------
    [ 4 bytes seq# big-endian ][ 16 bytes IV ][ AES-128-CBC ciphertext ]

* Each datagram carries its own random IV so it can be decrypted on
  its own -- UDP can drop or reorder packets and the rest of the stream
  still decodes (which is exactly the property MPEG-TS-over-UDP needs).
* Plaintext chunk size is at most 1316 bytes (7 x 188-byte MPEG-TS
  packets). We use os.read on FFmpeg's stdout so each datagram carries
  whatever is available right now, never waiting to fill a full
  1316-byte buffer.  With PKCS#7 padding a full chunk grows to
  1328 bytes, so the largest datagram is 4 + 16 + 1328 = 1348 bytes --
  comfortably under a 1500-byte Ethernet MTU.
* End-of-stream is signalled by a sentinel datagram where the sequence
  number field equals 0xFFFFFFFF and the rest of the packet is empty.
  The sentinel is sent a few times to survive UDP loss.  It is sent
  from a `finally` block so the receiver always wakes up cleanly, even
  if the sender fails partway through.
"""
from __future__ import annotations

import logging
import os
import socket
import struct
from typing import IO

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import padding as asym_padding
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.primitives.padding import PKCS7

from .base import TransportPlugin

logger = logging.getLogger(__name__)

_AES_KEY_SIZE = 16          # AES-128 -> 16-byte key
_AES_BLOCK_SIZE = 16        # AES block size is always 128 bits = 16 bytes
_MAX_PLAINTEXT_CHUNK = 1316 # UDP payload cap (7 x 188-byte MPEG-TS packets)
_CTRL_OFFSET = 1            # control channel lives at port + 1 (TCP)
_EOS_SEQ = 0xFFFFFFFF       # sentinel sequence number marking end-of-stream
_EOS_REPEATS = 5            # send EOS this many times so UDP loss can't hide it
_RECV_BUFFER = 65535        # max UDP datagram we ever read


class AESCBCUDPPlugin(TransportPlugin):
    """
    AES-128-CBC over UDP. Session key established via RSA-2048 PKCS#1 v1.5.
    Transport mode: pipe-based (both sender_url / receiver_url return None).
    """

    def __init__(self, config: dict) -> None:
        super().__init__(config)
        self._key: bytes | None = None
        # UDP socket for incoming data, bound during receiver_handshake so
        # no datagrams are missed while FFmpeg is starting up.
        self._data_sock: socket.socket | None = None
        # Idle timeout (seconds) for the receiver loop. Configurable via
        # the "plugin.idle_timeout" key in config.yaml; defaults to 10 s.
        self._idle_timeout: float = float(config.get("idle_timeout", 10.0))

    # ------------------------------------------------------------------
    # Tell sender.py / receiver.py we're a pipe-based plugin
    # ------------------------------------------------------------------

    def sender_url(self, host: str, port: int) -> None:   # type: ignore[override]
        return None

    def receiver_url(self, port: int) -> None:            # type: ignore[override]
        return None

    def post_handshake_delay(self) -> float:
        # Give the receiver time to spawn FFmpeg before the first UDP
        # datagram arrives.
        return 1.0

    # ------------------------------------------------------------------
    # Handshake -- RSA-PKCS1v15 key encapsulation over short TCP control
    # ------------------------------------------------------------------

    def receiver_handshake(self, port: int) -> None:
        """
        Generate an RSA-2048 keypair, send the public key to the sender,
        receive and decrypt the wrapped AES-128 session key, then bind
        the UDP data socket so packets sent during FFmpeg startup are
        buffered by the kernel rather than lost.
        """
        private_key = rsa.generate_private_key(
            public_exponent=65537, key_size=2048
        )
        pub_pem = private_key.public_key().public_bytes(
            serialization.Encoding.PEM,
            serialization.PublicFormat.SubjectPublicKeyInfo,
        )

        ctrl_port = port + _CTRL_OFFSET
        logger.info("Handshake: waiting on TCP ctrl port %d", ctrl_port)

        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as srv:
            srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            srv.bind(("0.0.0.0", ctrl_port))
            srv.listen(1)
            conn, addr = srv.accept()
            with conn:
                # Step 1 -- send public key (PEM, length-prefixed)
                _send_framed(conn, pub_pem)
                # Step 2 -- receive RSA-PKCS1v15-wrapped AES-128 key
                ciphertext = _recv_framed(conn)
                # Step 3 -- acknowledge
                conn.sendall(b"ACK")

        self._key = private_key.decrypt(ciphertext, asym_padding.PKCS1v15())
        if len(self._key) != _AES_KEY_SIZE:
            raise RuntimeError(
                f"Unwrapped key has wrong length: {len(self._key)} "
                f"(expected {_AES_KEY_SIZE})"
            )
        logger.info(
            "Handshake complete with %s -- AES-128 session key unwrapped",
            addr[0],
        )

        # Bind the UDP data socket now so the kernel buffers incoming
        # packets while FFmpeg is still starting up.
        self._data_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._data_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        # Generous receive buffer (4 MB) -- large enough to absorb a brief
        # FFmpeg startup hiccup at typical bitrates.
        try:
            self._data_sock.setsockopt(
                socket.SOL_SOCKET, socket.SO_RCVBUF, 4 * 1024 * 1024
            )
        except OSError:
            pass  # not fatal if the OS rejects the requested buffer size
        self._data_sock.bind(("0.0.0.0", port))

    def sender_handshake(self, host: str, port: int) -> None:
        """
        Connect to the receiver's TCP control port (with retry), receive
        its public key, generate a random AES-128 key, wrap it with the
        public key, and send it back.
        """
        ctrl_port = port + _CTRL_OFFSET
        logger.info("Handshake: connecting to %s:%d (TCP, with retry)",
                    host, ctrl_port)

        with self._connect_with_retry(host, ctrl_port) as s:
            # Step 1 -- receive the receiver's public key
            pub_pem = _recv_framed(s)
            # Step 2 -- pick an AES-128 key, wrap it, send it
            self._key = os.urandom(_AES_KEY_SIZE)
            pub_key = serialization.load_pem_public_key(pub_pem)
            wrapped = pub_key.encrypt(self._key, asym_padding.PKCS1v15())
            _send_framed(s, wrapped)
            # Step 3 -- wait for ACK
            ack = s.recv(3)
            if ack != b"ACK":
                raise RuntimeError(f"Unexpected handshake ack: {ack!r}")

        logger.info("Handshake complete -- AES-128 session key sent")

    # ------------------------------------------------------------------
    # Pipe-based transport over UDP
    # ------------------------------------------------------------------

    def sender_transport(self, stream: IO[bytes], host: str, port: int) -> None:
        """
        Read raw MPEG-TS from FFmpeg stdout (using os.read so we never
        sit idle waiting for a buffer to fill), AES-128-CBC-encrypt each
        chunk under a fresh random IV, and ship it as one UDP datagram.

        EOS sentinels are sent from a `finally` block so the receiver
        always wakes up cleanly -- even if encryption raised partway.
        """
        if self._key is None:
            raise RuntimeError("sender_transport called before handshake")

        seq = 0
        total_bytes = 0
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        # Larger send buffer absorbs short bitrate spikes without dropping.
        try:
            sock.setsockopt(
                socket.SOL_SOCKET, socket.SO_SNDBUF, 4 * 1024 * 1024
            )
        except OSError:
            pass

        logger.info("Sending AES-128-CBC datagrams to %s:%d (UDP)", host, port)

        # Use the underlying file descriptor with os.read so each call
        # returns whatever FFmpeg has produced so far in one syscall,
        # rather than blocking until a full _MAX_PLAINTEXT_CHUNK buffer
        # has accumulated. FFmpeg writes MPEG-TS in 188-byte units and
        # may produce them in small bursts at startup; with a buffered
        # read(N) we'd otherwise sit idle waiting for the buffer to fill.
        fd = stream.fileno()
        try:
            while True:
                try:
                    chunk = os.read(fd, _MAX_PLAINTEXT_CHUNK)
                except OSError as exc:
                    logger.warning("Read from FFmpeg stdout failed: %s", exc)
                    break
                if not chunk:
                    # Genuine EOF -- FFmpeg closed stdout (finished or died).
                    break

                iv = os.urandom(_AES_BLOCK_SIZE)
                try:
                    ciphertext = _aes_cbc_encrypt(self._key, iv, chunk)
                except Exception:
                    logger.exception(
                        "AES-CBC encryption failed for datagram seq=%d", seq
                    )
                    raise

                datagram = struct.pack(">I", seq) + iv + ciphertext
                sock.sendto(datagram, (host, port))
                seq += 1
                if seq == _EOS_SEQ:
                    seq = 0  # wrap before colliding with the EOS sentinel
                total_bytes += len(chunk)
        finally:
            # Always send EOS -- even on exception -- so the receiver can
            # stop cleanly instead of waiting for the idle timeout.
            try:
                eos = struct.pack(">I", _EOS_SEQ)
                for _ in range(_EOS_REPEATS):
                    sock.sendto(eos, (host, port))
            except OSError as exc:
                logger.warning("Failed to send EOS sentinel: %s", exc)
            sock.close()

        logger.info(
            "Encrypted stream fully sent (%d datagrams, %d plaintext bytes)",
            seq, total_bytes,
        )

    def receiver_transport(self, stream: IO[bytes], port: int) -> None:
        """
        Receive UDP datagrams, AES-128-CBC-decrypt each one, and write
        the recovered plaintext MPEG-TS to FFmpeg's stdin.  Returns when
        the EOS sentinel arrives or when the socket has been idle for
        ``idle_timeout`` seconds.
        """
        if self._key is None or self._data_sock is None:
            raise RuntimeError("receiver_transport called before handshake")

        sock = self._data_sock
        sock.settimeout(self._idle_timeout)
        logger.info(
            "Listening for AES-128-CBC UDP datagrams on port %d "
            "(idle timeout %.1fs)",
            port, self._idle_timeout,
        )

        received = 0
        decrypted_bytes = 0
        decrypt_failures = 0
        try:
            while True:
                try:
                    datagram, addr = sock.recvfrom(_RECV_BUFFER)
                except socket.timeout:
                    logger.warning(
                        "Receiver idle for %.1fs -- closing stream "
                        "(received %d datagrams, %d decrypt failures)",
                        self._idle_timeout, received, decrypt_failures,
                    )
                    break

                if len(datagram) < 4:
                    logger.debug("Ignoring short datagram (%d B)",
                                 len(datagram))
                    continue

                (seq,) = struct.unpack(">I", datagram[:4])
                if seq == _EOS_SEQ:
                    logger.info(
                        "EOS sentinel from %s -- %d datagrams "
                        "/ %d plaintext bytes received "
                        "(%d decrypt failures)",
                        addr[0], received, decrypted_bytes,
                        decrypt_failures,
                    )
                    break

                if len(datagram) < 4 + _AES_BLOCK_SIZE + _AES_BLOCK_SIZE:
                    # Need at least seq + IV + one ciphertext block.
                    logger.debug("Ignoring malformed datagram (%d B)",
                                 len(datagram))
                    continue

                iv = datagram[4:4 + _AES_BLOCK_SIZE]
                ciphertext = datagram[4 + _AES_BLOCK_SIZE:]

                try:
                    plaintext = _aes_cbc_decrypt(self._key, iv, ciphertext)
                except ValueError as exc:
                    # Bad padding usually means the datagram was corrupted
                    # in flight or the keys somehow disagree.  First few
                    # failures get a loud warning so the cause is obvious;
                    # after that we only log occasionally to avoid flooding.
                    decrypt_failures += 1
                    if decrypt_failures <= 3 or decrypt_failures % 100 == 0:
                        logger.warning(
                            "Datagram seq=%d failed to decrypt (#%d): %s",
                            seq, decrypt_failures, exc,
                        )
                    continue

                stream.write(plaintext)
                stream.flush()

                if received == 0:
                    logger.info(
                        "First datagram decrypted from %s "
                        "(seq=%d, %d ciphertext bytes -> %d plaintext bytes)",
                        addr[0], seq, len(ciphertext), len(plaintext),
                    )

                received += 1
                decrypted_bytes += len(plaintext)
        finally:
            sock.close()
            self._data_sock = None

        logger.info("Decryption complete")


# ---------------------------------------------------------------------------
# Crypto helpers
# ---------------------------------------------------------------------------

def _aes_cbc_encrypt(key: bytes, iv: bytes, plaintext: bytes) -> bytes:
    padder = PKCS7(_AES_BLOCK_SIZE * 8).padder()
    padded = padder.update(plaintext) + padder.finalize()
    encryptor = Cipher(algorithms.AES(key), modes.CBC(iv)).encryptor()
    return encryptor.update(padded) + encryptor.finalize()


def _aes_cbc_decrypt(key: bytes, iv: bytes, ciphertext: bytes) -> bytes:
    decryptor = Cipher(algorithms.AES(key), modes.CBC(iv)).decryptor()
    padded = decryptor.update(ciphertext) + decryptor.finalize()
    unpadder = PKCS7(_AES_BLOCK_SIZE * 8).unpadder()
    return unpadder.update(padded) + unpadder.finalize()


# ---------------------------------------------------------------------------
# Framing helpers -- 4-byte big-endian length prefix + payload (TCP control)
# ---------------------------------------------------------------------------

def _send_framed(sock: socket.socket, data: bytes) -> None:
    sock.sendall(struct.pack(">I", len(data)) + data)


def _recv_framed(sock: socket.socket) -> bytes:
    header = _recv_exactly(sock, 4)
    if not header:
        return b""
    return _recv_exactly(sock, struct.unpack(">I", header)[0])


def _recv_exactly(sock: socket.socket, n: int) -> bytes:
    buf = bytearray()
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            if not buf:
                return b""  # clean EOF at a frame boundary
            raise ConnectionError(
                f"Connection closed after {len(buf)}/{n} bytes"
            )
        buf.extend(chunk)
    return bytes(buf)
