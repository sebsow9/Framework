import logging
import subprocess
import time

from framework.plugins.loader import load_plugin

logger = logging.getLogger(__name__)


class Sender:
    def __init__(self, config: dict) -> None:
        self.network = config["network"]
        self.video = config["video"]
        self.startup_delay = config.get("sender", {}).get("startup_delay", 3) 
        self.plugin = load_plugin(config)

    def run(self) -> None:
        host = self.network["receiver_host"]
        port = self.network["port"]
        source = self.video["source"]
        codec = self.video.get("codec", "libx264")
        bitrate = self.video.get("bitrate", "2M")
        fps = self.video.get("fps", 30)

        # Base FFmpeg args that are the same regardless of transport mode
        ffmpeg_base = [
            "ffmpeg", "-hide_banner",
            "-re",              # read at native frame rate (real-time simulation)
            "-i", source,
            "-c:v", codec,
            "-b:v", bitrate,
            "-r", str(fps),
            "-c:a", "aac",     # MPEG-TS requires AAC audio
            "-f", "mpegts",
        ]

        if self.plugin:
            # --- Plugin path ---
            self.plugin.sender_handshake(host, port)

            delay = self.plugin.post_handshake_delay()
            if delay:
                logger.info("Waiting %.1fs for receiver FFmpeg to start...", delay)
                time.sleep(delay)

            url = self.plugin.sender_url(host, port)

            if url is None:
                # Pipe mode: plugin owns the byte stream
                self._run_pipe(ffmpeg_base, host, port)
            else:
                # URL mode: FFmpeg handles transport directly
                cmd = ffmpeg_base + self.plugin.sender_extra_args() + [url]
                self._run_ffmpeg(cmd, url)

        else:
            # --- Default UDP path (no plugin) ---
            logger.info("Waiting %ds for receiver to be ready...", self.startup_delay)
            time.sleep(self.startup_delay)
            destination = f"udp://{host}:{port}"
            self._run_ffmpeg(ffmpeg_base + [destination], destination)

    # ------------------------------------------------------------------

    def _run_ffmpeg(self, cmd: list[str], destination: str) -> None:
        logger.info("Streaming -> %s", destination)
        logger.debug("FFmpeg command: %s", " ".join(cmd))
        result = subprocess.run(cmd)
        if result.returncode != 0:
            logger.error("FFmpeg exited with code %d", result.returncode)
        else:
            logger.info("Stream finished.")

    def _run_pipe(self, ffmpeg_base: list[str], host: str, port: int) -> None:
        """
        Launch FFmpeg with stdout=PIPE, hand the stream to the plugin for
        encryption and transmission, then wait for FFmpeg to exit.
        """
        cmd = ffmpeg_base + ["pipe:1"]
        logger.info("Streaming (pipe) -> %s:%d via plugin", host, port)
        logger.debug("FFmpeg command: %s", " ".join(cmd))

        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE)
        try:
            self.plugin.sender_transport(proc.stdout, host, port)
        finally:
            proc.stdout.close()
            ret = proc.wait()

        if ret != 0:
            logger.error("FFmpeg exited with code %d", ret)
        else:
            logger.info("Stream finished.")
