import logging
import os
import subprocess

from framework.plugins.loader import load_plugin

logger = logging.getLogger(__name__)


class Receiver:
    def __init__(self, config: dict) -> None:
        self.network = config["network"]
        self.video = config.get("video", None)
        self.audio = config.get("audio", None)
        self.plugin = load_plugin(config)

    def run(self) -> None:
        port = self.network["port"]

        if self.video:
            output = self.video["output"]
        elif self.audio:
            output = self.audio["output"]

        os.makedirs(os.path.dirname(output), exist_ok=True)

        if self.plugin:
            # --- Plugin path ---
            self.plugin.receiver_handshake(port)

            url = self.plugin.receiver_url(port)

            if url is None:
                # Pipe mode: plugin owns the byte stream
                self._run_pipe(output, port)
            else:
                # URL mode: FFmpeg handles transport directly
                # TCP/TLS connections close cleanly → no fragmented MP4 needed
                cmd = self._ffmpeg_output_cmd(url, output, fragmented=False)
                cmd += self.plugin.receiver_extra_args()
                self._run_ffmpeg(cmd, url)

        else:
            # --- Default UDP path (no plugin) ---
            timeout_us = self.network.get("receiver_timeout_ms", 10000) * 1000
            source = f"udp://0.0.0.0:{port}?timeout={timeout_us}&overrun_nonfatal=1"
            # UDP has no clean EOF → use fragmented MP4 so file is valid if killed
            cmd = self._ffmpeg_output_cmd(source, output, fragmented=True)
            self._run_ffmpeg(cmd, source)

    # ------------------------------------------------------------------

    def _ffmpeg_output_cmd(
        self, source: str, output: str, fragmented: bool
    ) -> list[str]:
        cmd = [
            "ffmpeg", "-hide_banner",
            "-i", source,
            "-c", "copy",
            "-y",
        ]
        if fragmented:
            cmd += ["-movflags", "+frag_keyframe+empty_moov+default_base_moof"]  #TODO: look if audio needs different args
        cmd.append(output)
        return cmd

    def _run_ffmpeg(self, cmd: list[str], source: str) -> None:
        logger.info("Receiving from %s", source)
        logger.debug("FFmpeg command: %s", " ".join(cmd))
        result = subprocess.run(cmd)
        if result.returncode != 0:
            logger.error("FFmpeg exited with code %d", result.returncode)
        else:
            logger.info("Received video saved.")

    def _run_pipe(self, output: str, port: int) -> None:
        """
        Launch FFmpeg reading from stdin, hand its stdin to the plugin for
        decryption, then wait for FFmpeg to finish writing the output file.
        TCP sends a clean EOF when the sender disconnects, so FFmpeg exits
        gracefully and the output file is always valid.
        """
        cmd = [
            "ffmpeg", "-hide_banner",
            "-i", "pipe:0",
            "-c", "copy",
            "-y", output,
        ]
        logger.info("Receiving (pipe) via plugin -> %s", output)
        logger.debug("FFmpeg command: %s", " ".join(cmd))

        proc = subprocess.Popen(cmd, stdin=subprocess.PIPE)
        try:
            self.plugin.receiver_transport(proc.stdin, port)
        finally:
            proc.stdin.close()      # signal EOF to FFmpeg
            ret = proc.wait()       # let FFmpeg write and close the file

        if ret != 0:
            logger.error("FFmpeg exited with code %d", ret)
        else:
            if self.video:
                logger.info("Received video saved to %s", output)
            elif self.audio:
                logger.info("Received audio saved to %s", output)
