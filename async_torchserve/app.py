""" App to run asynchronous model servers """
import asyncio
import logging

from async_torchserve.config import config
from async_torchserve.model_server import ModelServer

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)


def main():
    loop = asyncio.get_event_loop()
    asyncio.set_event_loop(loop)
    model_servers = [
        ModelServer(model_config, loop, config["bootstrap_servers"])
        for model_config in config["models"]
    ]
    log.info("Starting all model servers")
    for model_server in model_servers:
        loop.run_until_complete(model_server.start())
    try:
        for model_server in model_servers:
            loop.run_until_complete(model_server.process())
    except KeyboardInterrupt:
        log.info("Model serving interrupted")
    finally:
        for model_server in model_servers:
            loop.run_until_complete(model_server.stop())
        loop.close()
        log.info("Shutdown all model servers successfully")


if __name__=="__main__":
    main()