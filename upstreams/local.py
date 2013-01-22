import tornado.iostream
import socket
import functools
from .upstream import Upstream


class LocalUpstream(Upstream):
    def do_init(self):
        self.socket = socket.socket(self.address_type, socket.SOCK_STREAM)
        self.stream = tornado.iostream.IOStream(self.socket)
        self.stream.set_close_callback(self.on_close)

    def do_connect(self):
        self.stream.connect(self.destination, self.on_connect)

    def local_address_type(self):
        return self.address_type

    def local_address(self):
        return self.socket.getsockname()

    def on_connect(self):
        self.debug("connected!")
        self.connection_callback(self)
        on_finish = functools.partial(self.on_streaming_data, finished=True)
        self.stream.read_until_close(on_finish, self.on_streaming_data)

    def on_close(self):
        if self.stream.error:
            self.debug("closed due to error: " + str(self.stream.error))
            self.error_callback(self, self.stream.error.errno)
        else:
            self.debug("closed")
            self.close_callback(self)

    def on_streaming_data(self, data, finished=False):
        if len(data):
            self.debug("received %d bytes of data." % len(data))
            self.streaming_callback(self, data)

    def do_send(self, data):
        try:
            self.stream.write(data)
        except IOError as e:
            self.debug("cannot write: %s" % str(e))
            self.stream.close()

    def do_close(self):
        self.stream.close()
