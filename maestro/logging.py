import logging
import os
from logging import DEBUG, INFO, WARNING, ERROR, FATAL, CRITICAL
import sidekick.api as sk

LOG_LEVELS = {DEBUG, INFO, WARNING, ERROR, FATAL, CRITICAL}


@sk.once
def config(level=INFO):
    log = logging.getLogger('maestro')

    if os.environ.get('DEBUG', '').lower() == 'true':
        level = DEBUG
    elif log_level := os.environ.get('LOG_LEVEL'):
        level = log_level.upper()
    log.setLevel(level)

    formatter = logging.Formatter('[%(levelname)s] %(message)s')
    ch = logging.StreamHandler()
    ch.setLevel(level)
    ch.setFormatter(formatter)

    log.addHandler(ch)
    log.debug('Starting maestro in debug mode.')

    return log


log = config()
