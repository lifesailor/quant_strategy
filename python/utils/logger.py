import os
import sys
import logging

base_path = os.path.dirname(os.path.abspath(os.path.dirname(__file__)))
log_path = os.path.join(base_path, 'log')


class Logger:
    def __init__(self):
        pass

    @staticmethod
    def set_logger(name):
        logger = logging.getLogger(name)
        logger.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

        stream_hander = logging.StreamHandler()
        stream_hander.setFormatter(formatter)
        logger.addHandler(stream_hander)

        filename = os.path.join(log_path, name + '.log')
        file_handler = logging.FileHandler(filename=filename)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
        return logger


if __name__ == "__main__":
    logger = Logger.set_logger("test")
    logger.info("logging test")
