import logging

def get_logger(name):
    log = logging.getLogger(name)
    log.setLevel(logging.INFO)

    console_handler = logging.StreamHandler()
    file_handler = logging.FileHandler(f'/tmp/log_{name}.log', mode='a')

    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(formatter)
    file_handler.setFormatter(formatter)

    log.addHandler(console_handler)
    log.addHandler(file_handler)

    return log
