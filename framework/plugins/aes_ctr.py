"""
AES-256-CTR pipe-based transport with RSA-OAEP key encapsulation.

Key exchange (RSA-OAEP)
------------------------
The AES session key is never sent in plaintext.

1. Receiver generates a fresh RSA-2048 keypair.
2. Receiver sends the *public* key to the sender (safe to share).
3. Sender generates a random 32-byte AES key + 16-byte CTR nonce.
4. Sender encrypts (key ‖ nonce) with the receiver's public key using
   RSA-OAEP / SHA-256 and sends the ciphertext.
5. Receiver decrypts the ciphertext with its private key.
6. Both sides now share the same AES key and nonce — neither was ever
   visible on the wire.

  Receiver                          Sender
    |                                  |
    | generate RSA-2048 keypair        |
    | listen on ctrl port (port + 1)   |
    |                                  | sender_handshake()
    |<------- TCP connect -----------  |
    |--- len(4B) + public key (PEM) -->|
    |<-- len(4B) + OAEP ciphertext ----|  wraps AES key + CTR nonce
    |--- b"ACK" --------------------> |
    |                                  |
    | private_key.decrypt() → key,nonce| sender already has key, nonce
    |                                  |
    | bind data socket (port)          | sleep post_handshake_delay (1 s)
    | start FFmpeg  stdin=PIPE         | start FFmpeg  stdout=PIPE
    |<====== encrypted MPEG-TS ========| sender_transport() → AES-CTR
    | receiver_transport() → AES-CTR  |
    | writes to FFmpeg stdin           |

Wire framing
------------
Every network message (public key, ciphertext, data frames) uses the
same 4-byte big-endian length prefix so the receiver always knows exactly
how many bytes to read.

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

_AES_KEY_SIZE = 32    # AES-256
_CTR_NONCE_SIZE = 16  # AES block size, used as CTR nonce
_CHUNK_SIZE = 65536   # 64 KB per data frame
_CTRL_OFFSET = 1      # control channel lives at port + 1

_OAEP = padding.OAEP(
    mgf=padding.MGF1(algorithm=hashes.SHA256()),
    algorithm=hashes.SHA256(),
    label=None,
)


class AESCTRPlugin(TransportPlugin):
    """
    AES-256-CTR over TCP.  Session key established via RSA-2048 OAEP.
    Transport mode: pipe-based (both sender_url / receiver_url return None).
    """

    def __init__(self, config: dict) -> None:
        super().__init__(config)
        self._key: bytes | None = None
        self._nonce: bytes | None = None
        # Bound and listening after receiver_handshake; consumed in receiver_transport
        self._data_server: socket.socket | None = None

    # ------------------------------------------------------------------
    # Signal pipe mode to sender / receiver
    # ------------------------------------------------------------------

    def sender_url(self, host: str, port: int) -> None:   # type: ignore[override]
        return None

    def receiver_url(self, port: int) -> None:            # type: ignore[override]
        return None

    def post_handshake_delay(self) -> float:
        # Let receiver start FFmpeg and bind the data socket before we connect
        return 1.0

    # ------------------------------------------------------------------
    # Handshake — RSA-OAEP key encapsulation
    # ------------------------------------------------------------------

    def receiver_handshake(self, port: int) -> None:
        """
        Generate RSA keypair → send public key → receive and decrypt the
        AES session key that the sender wrapped with it.
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
                # Step 2 — receive RSA_OAEP wrapped session key
                ciphertext = _recv_framed(conn)
                # Step 3 — acknowledge
                conn.sendall(b"ACK")

        session_material = private_key.decrypt(ciphertext, _OAEP)
        self._key = session_material[:_AES_KEY_SIZE]
        self._nonce = session_material[_AES_KEY_SIZE:]
        logger.info("Handshake complete with %s — AES session key unwrapped", addr[0])

        # Bind data socket while FFmpeg starts up
        self._data_server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._data_server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._data_server.bind(("0.0.0.0", port))
        self._data_server.listen(1)

    def sender_handshake(self, host: str, port: int) -> None:
        """
        Receive the receiver's RSA public key → generate AES session key →
        encrypt and send it back.
        """
        ctrl_port = port + _CTRL_OFFSET
        logger.info("Handshake: connecting to %s:%d (with retry)", host, ctrl_port)

        with self._connect_with_retry(host, ctrl_port) as s:
            # Step 1 — receive public key
            pub_pem = _recv_framed(s)
            # Step 2 — generate session key, wrap with public key, send
            self._key = os.urandom(_AES_KEY_SIZE)
            self._nonce = os.urandom(_CTR_NONCE_SIZE)
            pub_key = serialization.load_pem_public_key(pub_pem)
            ciphertext = pub_key.encrypt(self._key + self._nonce, _OAEP)
            _send_framed(s, ciphertext)
            # Step 3 — wait for ACK
            ack = s.recv(3)
            if ack != b"ACK":
                raise RuntimeError(f"Unexpected handshake ack: {ack!r}")

        logger.info("Handshake complete — AES session key sent")

    # ------------------------------------------------------------------
    # Pipe-based transport
    # ------------------------------------------------------------------

    def sender_transport(self, stream: IO[bytes], host: str, port: int) -> None:
        """
        Read raw MPEG-TS from FFmpeg stdout, AES-CTR-encrypt in 64 KB chunks,
        send each chunk as a length-prefixed frame over TCP.
        """
        encryptor = Cipher(
            algorithms.AES(self._key), modes.CTR(self._nonce)
        ).encryptor()

        logger.info("Connecting to receiver data port %s:%d", host, port)
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.connect((host, port))
            while True:
                chunk = stream.read(_CHUNK_SIZE)
                if not chunk:
                    break
                _send_framed(sock, encryptor.update(chunk))

        encryptor.finalize()  # no-op for CTR, called for correctness
        logger.info("Encrypted stream fully sent")

    def receiver_transport(self, stream: IO[bytes], port: int) -> None:
        """
        Accept sender's TCP connection, receive length-prefixed frames,
        AES-CTR-decrypt, and write plaintext MPEG-TS to FFmpeg stdin.
        """
        decryptor = Cipher(
            algorithms.AES(self._key), modes.CTR(self._nonce)
        ).decryptor()

        logger.info("Waiting for sender on data port %d", port)
        conn, addr = self._data_server.accept()
        self._data_server.close()
        self._data_server = None

        logger.info("Sender connected from %s — decrypting", addr[0])
        with conn:
            while True:
                frame = _recv_framed(conn)
                if not frame:
                    break
                stream.write(decryptor.update(frame))
                stream.flush()

        decryptor.finalize()
        logger.info("Decryption complete")


# ---------------------------------------------------------------------------
# Framing helpers  —  4-byte big-endian length prefix + payload
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