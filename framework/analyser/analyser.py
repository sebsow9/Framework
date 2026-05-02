import logging
import os
import subprocess

logger = logging.getLogger(__name__)


class Analyser:
    def __init__(self, config: dict) -> None:
        self.video = config.get("video", None)
        self.audio = config.get("audio", None)
        if self.video:
            self.reference = config["video"]["source"]
            self.distorted = config["video"]["output"]

            analyser_cfg = config["video"].get("analyser", {}) or {}
            self.vmaf_enabled = analyser_cfg.get("vmaf", True)
            # vmaf_v0.6.1   : default 1080p model (use for SD/HD content)
            # vmaf_4k_v0.6.1: model trained for 4K viewing distance (use for UHD content)
            self.vmaf_model = analyser_cfg.get("vmaf_model", "vmaf_v0.6.1")
            # libvmaf is single-threaded by default; using all available cores
            # cuts wall time on 4K content from minutes to seconds.
            self.vmaf_threads = analyser_cfg.get("vmaf_threads", os.cpu_count() or 1)

        elif self.audio:
            self.reference = config["audio"]["source"]
            self.distorted = config["audio"]["output"]

    def run(self) -> dict:
        if not os.path.exists(self.distorted):
            logger.error("Distorted file not found: %s", self.distorted)
            return {}

        if self.video:
            # Single ffmpeg pass: decode each file once, fan out to all metrics
            # in parallel via `split`. Saves two full decodes of the source pair.
            results = self._run_video_metrics()
            self._log_video_results(results)
            return results
        elif self.audio:
            results = self._run_audio_metrics()
            self._log_audio_results(results)
            return results
    # ------------------------------------------------------------------

    def _run_video_metrics(self) -> dict:
        # input 0 = reference, input 1 = distorted.
        # libvmaf wants [distorted][reference]; psnr/ssim are symmetric.
        if self.vmaf_enabled:
            # The model value contains '=', which is a reserved char in
            # FFmpeg filter args. Single-quote it so the parser keeps it
            # whole instead of treating ':n_threads=...' as a runaway value.
            vmaf_filter = (
                f"libvmaf=model='version={self.vmaf_model}'"
                f":n_threads={self.vmaf_threads}"
            )
            filter_complex = (
                "[0:v]split=3[r1][r2][r3];"
                "[1:v]split=3[d1][d2][d3];"
                "[r1][d1]psnr[ps];"
                "[r2][d2]ssim[ss];"
                f"[d3][r3]{vmaf_filter}[vm]"
            )
            maps = ["-map", "[ps]", "-map", "[ss]", "-map", "[vm]"]
        else:
            filter_complex = (
                "[0:v]split=2[r1][r2];"
                "[1:v]split=2[d1][d2];"
                "[r1][d1]psnr[ps];"
                "[r2][d2]ssim[ss]"
            )
            maps = ["-map", "[ps]", "-map", "[ss]"]

        cmd = [
            "ffmpeg", "-hide_banner",
            "-i", self.reference,
            "-i", self.distorted,
            "-filter_complex", filter_complex,
            *maps,
            "-f", "null", "-",
        ]
        stderr = self._run(cmd)

        results: dict = {"psnr": None, "ssim": None}
        if self.vmaf_enabled:
            results["vmaf"] = None

        if stderr is None:
            if self.vmaf_enabled:
                logger.error(
                    "VMAF was enabled but the analyser ffmpeg pass failed. "
                    "Check that this image's ffmpeg is built with libvmaf "
                    "(`ffmpeg -filters | grep '^ ... libvmaf '`)."
                )
            return results

        for line in stderr.splitlines():
            if results["psnr"] is None and "average:" in line:
                try:
                    results["psnr"] = float(line.split("average:")[1].split()[0])
                except (IndexError, ValueError):
                    pass
            if results["ssim"] is None and "SSIM" in line and "All:" in line:
                try:
                    results["ssim"] = float(line.split("All:")[1].split()[0])
                except (IndexError, ValueError):
                    pass
            if self.vmaf_enabled and results.get("vmaf") is None and "VMAF score:" in line:
                try:
                    results["vmaf"] = float(line.split("VMAF score:")[1].strip())
                except (IndexError, ValueError):
                    pass

        if results["psnr"] is None:
            logger.warning("Could not parse PSNR from FFmpeg output")
        if results["ssim"] is None:
            logger.warning("Could not parse SSIM from FFmpeg output")
        if self.vmaf_enabled and results.get("vmaf") is None:
            logger.warning("Could not parse VMAF score from FFmpeg output")

        return results

    def _log_video_results(self, results: dict) -> None:
        logger.info("=== Video Quality Analysis Results ===")
        if results.get("psnr") is not None:
            logger.info("PSNR:  %.2f dB  (higher is better; >40 dB = excellent)", results["psnr"])
        if results.get("ssim") is not None:
            logger.info("SSIM:  %.6f   (1.0 = identical)", results["ssim"])
        if self.vmaf_enabled:
            if results.get("vmaf") is not None:
                logger.info("VMAF:  %.2f     (0-100; >90 = transparent)", results["vmaf"])
            else:
                logger.error("VMAF:  FAILED — see ffmpeg error above")

    def _run_audio_metrics(self) -> dict:
        filter_complex = (
            "[0:a]asplit=2[ref1][ref2];"
            "[1:a]asplit=2[dist1][dist2];"
            "[ref1][dist1]apsnr[apsnr];"
            "[ref2][dist2]asdr[asdr]"
        )
        maps = ["-map", "[apsnr]", "-map", "[asdr]"]
        cmd = [
            "ffmpeg", "-hide_banner",
            "-i", self.reference,
            "-i", self.distorted,
            "-filter_complex", filter_complex,
            *maps,
            "-f", "null", "-",
        ]
        stderr = self._run(cmd)

        results: dict = {"psnr": None, "sdr": None}
        psnr = []
        sdr = []
        if stderr is None:
            logger.error("_run_audio_metrics(): FFMPEG did not return results")
        for line in stderr.splitlines():
            if results["psnr"] is None and "PSNR" in line:
                psnr.append(float(line.split(":")[1].strip().split()[0]))
            if results["sdr"] is None and "SDR" in line:
                sdr.append(float(line.split(":")[1].strip().split()[0]))
        results["psnr"] = self.list_average(psnr)
        results["sdr"] = self.list_average(sdr)
        return results

    def _log_audio_results(self, results: dict) -> None:
        logger.info("+--- Audio Quality Analysis Results ---")
        if results.get("psnr") is not None:
            logger.info("| PSNR (channel average):  %.2f dB  (higher is better; >50 dB is generally indistinguishable from source. Interpretation varries across codecs, compression, etc.)", results["psnr"])
        if results.get("sdr") is not None:
            logger.info("| SDR (channel average): %.2f dB  (higher is better)", results["sdr"])
        logger.info("+--------------------------------------")

    def _run(self, cmd: list[str]) -> str | None:
        logger.debug("FFmpeg command: %s", " ".join(cmd))
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0:
            logger.error("FFmpeg exited with code %d\n%s", result.returncode, result.stderr)
            return None
        return result.stderr
    
    def list_average(self, list: list[float]) -> float:
        x = 0.0
        [x := x+item for item in list]
        return x/len(list)
