import logging
import functools


class LoggingEnabledObject(object):
    def __init__(self, fmt, args):
        self._log_fmt = fmt
        self._log_args = args
        self._log_prefix = fmt % args
        self.setup_logging()

    def setup_logging(self):
        @functools.wraps(logging.info)
        def info(fmt, *args, **kwargs):
            return logging.info(self._log_prefix + fmt, *args, **kwargs)
        self.info = info

        @functools.wraps(logging.debug)
        def debug(fmt, *args, **kwargs):
            return logging.debug(self._log_prefix + fmt, *args, **kwargs)
        self.debug = debug

        def error(fmt, *args, **kwargs):
            return logging.error(self._log_prefix + fmt, *args, **kwargs)
        self.error = error

        def warning(fmt, *args, **kwargs):
            return logging.warning(self._log_prefix + fmt, *args, **kwargs)
        self.warning = warning
