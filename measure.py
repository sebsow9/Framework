import os
import sys
import logging
import yaml

from framework.sender.sender import Sender
from framework.receiver.receiver import Receiver

# TO DO - add actual comparison between input and output
def setup_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )


def load_config(path: str = "config.yaml") -> dict:
    with open(path) as f:
        return yaml.safe_load(f)


def main() -> None:
    config = load_config()
    setup_logging(config.get("logging", {}).get("level", "INFO"))

    # ROLE env var takes precedence over config file
    role = os.environ.get("ROLE", config.get("role", "sender")).lower()

    if role == "sender":
        Sender(config).run()
    elif role == "receiver":
        Receiver(config).run()
    else:
        print(f"Unknown role: {role!r}. Must be 'sender' or 'receiver'.", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
