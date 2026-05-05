"""
AES-256-CTR pipe-based UDP transport with RSA-OAEP key encapsulation.

Key exchange (RSA-OAEP, over TCP control channel)
-------------------------------------------------
The AES session key is never sent in plaintext.

1. Receiver generates a fresh RSA-2048 keypair.
2. Receiver sends the *public* key to the sender (safe to share).
3. Sender generates a random 32-byte AES key + 16-byte base nonce.
4. Sender encrypts (key ‖ nonce) with the receiver's public key using
   RSA-OAEP / SHA-256 and sends the ciphertext.
5. Receiver decrypts the ciphertext with its private key.
6. Both sides now share the same AES key and base nonce.

Data path (UDP)
---------------
Encrypted media flows as length-implicit UDP datagrams:

    [seq : 8B big-endian][AES-CTR ciphertext]

Each datagram is encrypted independently with a per-packet IV derived from
the session base nonce and the sequence number:

    iv_i = base_nonce XOR (seq_i << 16)

Shifting seq up by 16 bits reserves the low 16 bits of the 128-bit CTR
state for the in-packet block counter. With 16-byte AES blocks that gives
each packet up to 65 536 blocks (1 MiB) of keystream before its counter
range would overlap the next packet's — far more than any UDP datagram
can carry. The result: loss, reordering, or duplication of one datagram
never desynchronises the rest of the stream, which is exactly what
FFmpeg's MPEG-TS demuxer expects from native UDP transport.

End of stream: the sender emits a FIN datagram (seq = 2**64 − 1, no
payload) ``_FIN_REPEATS`` times to survive UDP loss. The receiver also
arms a recv timeout so a lost FIN still terminates the loop.

  Receiver                          Sender
    |                                  |
    | listen on ctrl port (port + 1)   |
    |<------- TCP connect ------------ |  sender_handshake()
    |--- 4B len + public key (PEM) --->|
    |<-- 4B len + OAEP ciphertext -----|  wraps AES key + base nonce
    |--- "ACK" ---------------------->|
    |                                  |
    | bind UDP data socket (port)      | post_handshake_delay (1 s)
    |                                  |
    |<====== encrypted MPEG-TS ========|  per-datagram AES-CTR
    |  [seq][ciphertext]   (UDP)       |
    |                                  |
    |<-- FIN datagram (seq = 2^64-1) --|  repeated _FIN_REPEATS times
    | break loop, close FFmpeg stdin   |

Note: AES-CTR alone provides confidentiality but not authentication. A
network attacker who knows the protocol could inject malformed datagrams
that decrypt to garbage; FFmpeg's demuxer will simply drop them. Add an
HMAC or switch to AES-GCM if integrity is required.
"""
from __future__ import annotations

import logging
import os
import socket
import struct
from typing import IO

from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding, rsa
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes

from .base import TransportPlugin

logger = logging.getLogger(__name__)

_AES_KEY_SIZE = 32              # AES-256
_BASE_NONCE_SIZE = 16           # 128-bit session salt feeding per-packet IVs
_PACKET_PAYLOAD_SIZE = 1316     # 7 MPEG-TS packets; fits typical 1500-byte MTU
_PACKET_CTR_RESERVE_BITS = 16   # low bits of CTR reserved for in-packet block counter
_SEQ_HEADER = ">Q"              # 8-byte big-endian sequence number prefix
_SEQ_HEADER_SIZE = struct.calcsize(_SEQ_HEADER)
_FIN_SEQ = (1 << 64) - 1        # reserved seq value signalling end of stream
_FIN_REPEATS = 5                # send FIN this many times to mask UDP loss
_DEFAULT_RECV_TIMEOUT_MS = 10000
_CTRL_OFFSET = 1                # TCP control channel lives at port + 1
_RECV_BUF = 65536               # max UDP datagram size

_OAEP = padding.OAEP(
    mgf=padding.MGF1(algorithm=hashes.SHA256()),
    algorithm=hashes.SHA256(),
    label=None,
)


def _packet_iv(base_nonce: bytes, seq: int) -> bytes:
    """Per-packet AES-CTR IV: base nonce XOR'd with (seq << reserve_bits)."""
    base_int = int.from_bytes(base_nonce, "big")
    return (base_int ^ (seq << _PACKET_CTR_RESERVE_BITS)).to_bytes(16, "big")


class AESCTRPlugin(TransportPlugin):
    """
    AES-256-CTR over UDP. Session key established via RSA-2048 OAEP on TCP.
    Transport mode: pipe-based (both sender_url / receiver_url return None).
    """

    def __init__(self, config: dict) -> None:
        super().__init__(config)
        self._key: bytes | None = None
        self._nonce: bytes | None = None
        # Bound in receiver_handshake; consumed in receiver_transport.
        self._data_sock: socket.socket | None = None
        self._recv_timeout_s = config.get("recv_timeout_ms", _DEFAULT_RECV_TIMEOUT_MS) / 1000.0

    # ------------------------------------------------------------------
    # Signal pipe mode to sender / receiver
    # ------------------------------------------------------------------

    def sender_url(self, host: str, port: int) -> None:   # type: ignore[override]
        return None

    def receiver_url(self, port: int) -> None:            # type: ignore[override]
        return None

    def post_handshake_delay(self) -> float:
        # Let receiver start FFmpeg before the first datagram arrives.
        return 1.0

    # ------------------------------------------------------------------
    # Handshake — RSA-OAEP key encapsulation over TCP
    # ------------------------------------------------------------------

    def receiver_handshake(self, port: int) -> None:
        """
        Generate RSA keypair → send public key → receive and decrypt the
        AES session key that the sender wrapped with it. Then bind the
        UDP data socket so datagrams arriving during FFmpeg startup are
        kernel-buffered.
        """
        private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        pub_pem = private_key.public_key().public_bytes(
            serialization.Encoding.PEM,
            serialization.PublicFormat.SubjectPublicKeyInfo,
        )

        ctrl_port = port + _CTRL_OFFSET
        logger.info("Handshake: waiting on ctrl port %d", ctrl_port)

        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as srv:
            srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            srv.bind(("0.0.0.0", ctrl_port))
            srv.listen(1)
            conn, addr = srv.accept()
            with conn:
                # Step 1 — send public key
                _send_framed(conn, pub_pem)
                # Step 2 — receive RSA-OAEP wrapped session material
                ciphertext = _recv_framed(conn)
                # Step 3 — acknowledge
                conn.sendall(b"ACK")

        session_material = private_key.decrypt(ciphertext, _OAEP)
        self._key = session_material[:_AES_KEY_SIZE]
        self._nonce = session_material[_AES_KEY_SIZE:]
        logger.info("Handshake complete with %s — AES session key unwrapped", addr[0])

        # Bind UDP data socket while sender is in post_handshake_delay so
        # the kernel buffers any datagrams that arrive before FFmpeg starts.
        self._data_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._data_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._data_sock.bind(("0.0.0.0", port))

    def sender_handshake(self, host: str, port: int) -> None:
        """
        Receive the receiver's RSA public key → generate AES session key
        and base nonce → encrypt and send them back.
        """
        ctrl_port = port + _CTRL_OFFSET
        logger.info("Handshake: connecting to %s:%d (with retry)", host, ctrl_port)

        with self._connect_with_retry(host, ctrl_port) as s:
            # Step 1 — receive public key
            pub_pem = _recv_framed(s)
            # Step 2 — generate session material, wrap with public key, send
            self._key = os.urandom(_AES_KEY_SIZE)
            self._nonce = os.urandom(_BASE_NONCE_SIZE)
            pub_key = serialization.load_pem_public_key(pub_pem)
            ciphertext = pub_key.encrypt(self._key + self._nonce, _OAEP)
            _send_framed(s, ciphertext)
            # Step 3 — wait for ACK
            ack = s.recv(3)
            if ack != b"ACK":
                raise RuntimeError(f"Unexpected handshake ack: {ack!r}")

        logger.info("Handshake complete — AES session key sent")

    # ------------------------------------------------------------------
    # Pipe-based UDP transport
    # ------------------------------------------------------------------

    def sender_transport(self, stream: IO[bytes], host: str, port: int) -> None:
        """
        Read raw MPEG-TS from FFmpeg stdout, AES-CTR-encrypt each
        ``_PACKET_PAYLOAD_SIZE`` chunk under a per-packet IV, and emit
        ``[seq][ciphertext]`` UDP datagrams. Finishes by sending a FIN
        datagram a few times to survive packet loss.
        """
        logger.info("Sending UDP datagrams to %s:%d", host, port)
        seq = 0
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            # connect() sets the default destination and lets the kernel
            # surface ICMP "port unreachable" as an exception on send.
            sock.connect((host, port))

            while True:
                chunk = stream.read(_PACKET_PAYLOAD_SIZE)
                if not chunk:
                    break
                iv = _packet_iv(self._nonce, seq)
                cipher = Cipher(algorithms.AES(self._key), modes.CTR(iv)).encryptor()
                ct = cipher.update(chunk) + cipher.finalize()
                sock.send(struct.pack(_SEQ_HEADER, seq) + ct)
                seq += 1

            fin = struct.pack(_SEQ_HEADER, _FIN_SEQ)
            for _ in range(_FIN_REPEATS):
                sock.send(fin)

        logger.info("Encrypted stream fully sent (%d datagrams + FIN)", seq)

    def receiver_transport(self, stream: IO[bytes], port: int) -> None:
        """
        Read UDP datagrams, decrypt each independently with the per-packet
        IV, and write plaintext to FFmpeg stdin. Stops on FIN or after
        ``recv_timeout_ms`` of silence.
        """
        sock = self._data_sock
        self._data_sock = None
        sock.settimeout(self._recv_timeout_s)
        logger.info(
            "Listening for UDP datagrams on port %d (timeout %.1fs)",
            port, self._recv_timeout_s,
        )

        delivered = 0
        try:
            while True:
                try:
                    datagram, addr = sock.recvfrom(_RECV_BUF)
                except socket.timeout:
                    logger.warning(
                        "UDP recv timed out after %.1fs — stopping",
                        self._recv_timeout_s,
                    )
                    break

                if len(datagram) < _SEQ_HEADER_SIZE:
                    continue
                seq = struct.unpack(_SEQ_HEADER, datagram[:_SEQ_HEADER_SIZE])[0]
                if seq == _FIN_SEQ:
                    logger.info("FIN received from %s — stopping", addr[0])
                    break

                ct = datagram[_SEQ_HEADER_SIZE:]
                iv = _packet_iv(self._nonce, seq)
                cipher = Cipher(algorithms.AES(self._key), modes.CTR(iv)).decryptor()
                stream.write(cipher.update(ct) + cipher.finalize())
                stream.flush()
                delivered += 1
        finally:
            sock.close()

        logger.info("Decryption complete — %d datagrams written", delivered)


# ---------------------------------------------------------------------------
# Framing helpers — 4-byte big-endian length prefix + payload (handshake only)
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
            raise ConnectionError(f"Connection closed after {len(buf)}/{n} bytes")
        buf.extend(chunk)
    return bytes(buf)