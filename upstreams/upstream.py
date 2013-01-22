import socket
import tornado.ioloop
from objectlog import LoggingEnabledObject


class Upstream(LoggingEnabledObject):
    @classmethod
    def on_load(cls):
        pass

    def __init__(self, destination, address_type, connection_callback,
            error_callback, streaming_callback, close_callback, manager=None):
        super(Upstream, self).__init__(
            self.__class__.__name__ + "(%s:%d)  ", destination)
        assert address_type in [socket.AF_INET, socket.AF_INET6]
        self.manager = manager
        self.io_loop = tornado.ioloop.IOLoop.instance()
        self.destination = destination
        self.address_type = address_type
        self.connection_callback = connection_callback
        self.streaming_callback = streaming_callback
        self.error_callback = error_callback
        self.close_callback = close_callback
        self.debug("created.")
        self.do_init()
        self.do_connect()

    def do_init(self):
        pass

    def do_connect(self):
        raise NotImplemented("subclass of Upstream must implement do_connect")

    def do_close(self):
        raise NotImplemented("subclass of Upstream must implement do_close")

    def do_send(self, data):
        raise NotImplemented("subclass of Upstream must implement do_send")

    def send(self, data):
        self.do_send(data)
        self.debug("sent %d bytes of data." % len(data))

    def close(self):
        self.debug("closing...")
        self.do_close()
