import logging
import socket
import functools
import tornado.iostream


class LoggingEnabledObject(object):
    def __init__(self, fmt, args):
        self._log_fmt = fmt
        self._log_args = args
        self._log_prefix = fmt % args
        self.setup_logging()

    def setup_logging(self):
        def info(fmt, *args, **kwargs):
            return logging.info(self._log_prefix + fmt, *args, **kwargs)
        self.info = info

        def debug(fmt, *args, **kwargs):
            return logging.debug(self._log_prefix + fmt, *args, **kwargs)
        self.debug = debug

        def error(fmt, *args, **kwargs):
            return logging.error(self._log_prefix + fmt, *args, **kwargs)
        self.error = error

        def warning(fmt, *args, **kwargs):
            return logging.warning(self._log_prefix + fmt, *args, **kwargs)
        self.warning = warning


class Upstream(LoggingEnabledObject):
    def __init__(self, destination, address_type, connection_callback,
            error_callback, streaming_callback, close_callback):
        super(Upstream, self).__init__(
            self.__class__.__name__ + "(%s:%d)  ", destination)
        assert address_type in [socket.AF_INET, socket.AF_INET6]
        self.destination = destination
        self.address_type = address_type
        self.connection_callback = connection_callback
        self.streaming_callback = streaming_callback
        self.error_callback = error_callback
        self.close_callback = close_callback
        self.do_connect()

    def do_connect(self):
        raise NotImplemented("subclass of Upstream must implement do_connect")


class LocalUpstream(Upstream):
    def do_connect(self):
        self.socket = socket.socket(self.address_type, socket.SOCK_STREAM)
        self.stream = tornado.iostream.IOStream(self.socket)
        self.stream.set_close_callback(self.on_close)
        self.stream.connect(self.destination, self.on_connect)
        self.can_apply_callback = True
        self.debug("created.")

    def local_address_type(self):
        return self.address_type

    def local_address(self):
        return self.socket.getsockname()

    def on_connect(self):
        self.debug("connected!")
        self.connection_callback()
        on_finish = functools.partial(self.on_streaming_data, finished=True)
        self.stream.read_until_close(on_finish, self.on_streaming_data)

    def on_close(self):
        if self.stream.error:
            self.debug("closed due to error: " + str(self.stream.error))
            self.error_callback(self.stream.error.errno)
        else:
            self.debug("closed")
            self.close_callback()

    def on_streaming_data(self, data, finished=False):
        if len(data):
            self.debug("received %d bytes of data." % len(data))
            self.streaming_callback(data)

    def apply_callback(self, callback, *args, **kwargs):
        if self.can_apply_callback:
            callback(*args, **kwargs)

    def send(self, data):
        self.stream.write(data)
        self.debug("sent %d bytes of data." % len(data))

    def close(self, no_more_callbacks=False):
        self.stream.close()
