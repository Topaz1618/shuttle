#!/usr/bin/env python

# ugly fix for loading objectlog
import sys
import os
sys.path.insert(0, os.path.abspath('.'))

import tornado.ioloop
import tornado.iostream
import tornado.options
import tornado.process
import tornado.netutil
import socket
import errno
import functools
import struct
from objectlog import LoggingEnabledObject


class SOCKSConnection(LoggingEnabledObject):
    COMMAND_MAP = {
            0x01:   'CONNECT',
            0x02:   'BIND',
            0x03:   'UDP ASSOCIATION'
        }
    ACCEPTED_COMMANDS = [0x01, ]
    ADDRESS_TYPE_MAP = {
            0x01:   'IPv4 Address',
            0x03:   'Domain name',
            0x04:   'IPv6 Address'
        }
    ADDRESS_TYPE_LENGTH = {
            0x01:   4,
            0x04:   16
        }
    ACCEPTED_ADDRESS_TYPES = [0x01, 0x03, 0x04]
    REPLY_CODES = {
            0x00:   'succeeded',
            0x01:   'general SOCKS server failure',
            0x03:   'Network unreachable',
            0x04:   'Host unreachable',
            0x05:   'Connection refused',
            0x07:   'Command not supported',
            0x08:   'Address type not supported',
        }
    ERRNO_MAP = {
            errno.ECONNREFUSED:     0x05,
            errno.EHOSTUNREACH:     0x04,
            errno.ENETUNREACH:      0x03,
        }

    def __init__(self, conn, addr, upstream_cls=None, manager=None):
        super(SOCKSConnection, self).__init__("Proxy(%s:%d)  ", addr)
        self.conn = conn
        self.addr = addr
        self.manager = manager
        if upstream_cls is None:
            raise TypeError("must specify an upstrea class!")
        self.upstream_cls = upstream_cls
        self.setup_logging()
        self.on_connected()
        self.destination = None
        self.sent_reply = False

    def on_connected(self):
        self.debug("connected!")
        self.conn.set_close_callback(self.on_connection_close)
        self.conn.read_bytes(2, self.on_auth_num_methods)

    def on_connection_close(self):
        self.debug("disconnected!")
        self.clean_upstream()

    def on_auth_num_methods(self, data):
        # ver + nmethods. ref: rfc1928. P3
        self.ver, self.auth_nmethods = struct.unpack("!BB", data)
        self.debug("version: 0x%X, number of methods: %d" %
            (self.ver, self.auth_nmethods))
        if self.ver != 0x05:
            self.warning("version mismatch, closing connection!")
            self.conn.close()
        else:
            self.conn.read_bytes(self.auth_nmethods, self.on_auth_methods)

    def on_auth_methods(self, data):
        self.auth_methods = struct.unpack("B" * self.auth_nmethods, data)
        if 0x00 in self.auth_methods:
            self.conn.write(struct.pack("!BB", self.ver, 0x00))
            self.on_auth_finished()
        else:
            self.warning("the client does not support \"no authentication\", \
                closing connection!")
            self.conn.close()

    def on_auth_finished(self):
        self.debug("authentication finished.")
        self.wait_request()

    def wait_request(self):
        self.conn.read_bytes(4, self.on_request_cmd_address_type)

    def wait_for_domain_name(self):
        self.raw_dest_addr = ""
        self.conn.read_bytes(1, self.on_domain_name_num_octets)

    def on_domain_name_num_octets(self, data):
        self.raw_dest_addr += data
        num, = struct.unpack("!B", data)
        self.conn.read_bytes(num, self.on_domain_name_octets)

    def on_domain_name_octets(self, data):
        self.raw_dest_addr += data
        self.domain_name = data
        self.on_domain_name_complete()

    def on_domain_name_complete(self):
        self.debug("parsed domain name: %s" % self.domain_name)
        self.dest_addr = self.domain_name
        self.wait_destination_port()

    def on_request_cmd_address_type(self, data):
        _ver, self.cmd, _rsv, self.atyp = struct.unpack("!BBBB", data)
        self.debug("command: %s, address type: %s" % (
            self.COMMAND_MAP.get(self.cmd, "UNKNOWN COMMAND"),
            self.ADDRESS_TYPE_MAP.get(self.atyp, "UNKNOWN ADDRESS TYPE")))

        if self.cmd not in self.ACCEPTED_COMMANDS:
            self.write_reply(0x07)
            self.conn.close()

        if self.atyp not in self.ACCEPTED_ADDRESS_TYPES:
            self.write_reply(0x08)
            self.conn.close()

        if self.atyp in self.ADDRESS_TYPE_LENGTH:
            self.conn.read_bytes(self.ADDRESS_TYPE_LENGTH[self.atyp],
                self.on_request_fixed_length_address)
        else:
            self.wait_for_domain_name()

    def wait_destination_port(self):
        self.conn.read_bytes(2, self.on_destination_port)

    def convert_readable_address(self, addr):
        return socket.inet_ntop(socket.AF_INET if self.atyp == 0x01
            else socket.AF_INET6, addr)

    def convert_machine_address(self, addr_type, addr):
        return socket.inet_pton(addr_type, addr)

    def on_request_fixed_length_address(self, data):
        self.raw_dest_addr = data
        self.dest_addr = self.convert_readable_address(data)
        self.debug("received address: %s" % self.dest_addr)
        self.wait_destination_port()

    def on_destination_port(self, data):
        self.raw_dest_port = data
        port, = struct.unpack("!H", data)
        self.destination = (self.dest_addr, port)
        self.debug("received port: %s" % port)
        self.on_request_received()

    def on_request_received(self):
        self.info("received request: " + self.COMMAND_MAP[
            self.cmd] + ", destination: %s:%d" % self.destination)

        self.command_processors = {
            0x01:   self.do_connect,
        }
        self.command_processors[self.cmd]()

    def do_connect(self):
        self.upstream = self.upstream_cls(self.destination,
            socket.AF_INET6 if self.atyp == 0x04 else socket.AF_INET,
            self.on_upstream_connect, self.on_upstream_error,
            self.on_upstream_data, self.on_upstream_close, manager=manager)

    def on_upstream_connect(self, _dummy):
        addr_type = self.upstream.local_address_type()
        addr = self.upstream.local_address()
        self.debug("upstream connected from %s:%d" % addr)

        src_addr = self.convert_machine_address(addr_type, addr[0])
        self.write_reply(0x00, src_addr + struct.pack("!H", addr[1]))

        on_finish = functools.partial(self.on_socks_data, finished=True)
        self.conn.read_until_close(on_finish, self.on_socks_data)

    def on_upstream_error(self, _dummy, no):
        self.debug("upstream error: " + os.strerror(no))
        if not self.sent_reply:
            self.write_reply(self.ERRNO_MAP.get(no, 0x01))
        self.conn.close()

    def on_upstream_data(self, _dummy, data):
        try:
            self.conn.write(data)
            self.debug("transported %d bytes of data from upstream." % len(data))
        except IOError as e:
            self.debug("cannot write: %s" % str(e))
            self.upstream.close()

    def on_upstream_close(self, _dummy):
        self.conn.close()
        self.debug("upstream closed.")
        self.upstream = None

    def clean_upstream(self):
        if getattr(self, "upstream", None):
            self.upstream.close()
            self.upstream = None

    def on_socks_data(self, data, finished=False):
        if len(data):
            self.upstream.send(data)
            self.debug("transported %d bytes of data to upstream." % len(data))

    def on_socks_finish(self, _dummy):
        pass

    def write_reply(self, code, data=None):
        address_type = self.atyp
        if data is None:
            if self.destination:
                data = self.raw_dest_addr + self.raw_dest_port
            else:
                data = struct.pack("!BLH", 0x01, 0x00, 0x00)
        else:
            if self.atyp == 0x03:
                address_type = 0x01
        self.conn.write(struct.pack("!BBBB", self.ver, code, 0x00, address_type
            ) + data)
        self.debug("sent reply: %s" % (self.REPLY_CODES.get(
            code, "UNKNOWN REPLY")))
        self.sent_reply = True


class SOCKSServer(tornado.netutil.TCPServer):
    def __init__(self, upstream_cls, manager):
        super(SOCKSServer, self).__init__()
        self.connections = []
        self.upstream_cls = upstream_cls
        self.manager = manager

    def handle_stream(self, stream, address):
        conn = SOCKSConnection(stream, address, upstream_cls=upstream_cls,
            manager=self.manager)
        self.on_new_connection(conn)

    def on_new_connection(self, conn):
        self.connections.append(conn)


def main(num_processes, addr, port, upstream_cls, manager):
    server = SOCKSServer(upstream_cls, manager)
    server.bind(port, address=addr, backlog=1024)
    try:
        server.start(num_processes)
        tornado.ioloop.IOLoop.instance().start()
    except KeyboardInterrupt:
        tornado.ioloop.IOLoop.instance().stop()
        if manager:
            manager.cleanup()
    return 0


def import_class(cl):
    d = cl.rfind(".")
    classname = cl[d + 1: len(cl)]
    m = __import__(cl[0:d], globals(), locals(), [classname])
    return getattr(m, classname)


if __name__ == '__main__':

    tornado.options.define("port", default=8890,
        help="Run SOCKS proxy on a specific port", type=int)
    tornado.options.define("processes", default=1,
        help="Run multiple processes", type=int)
    tornado.options.define("bind", default='0.0.0.0',
        help="Bind address", type=str)
    tornado.options.define("upstream", default="upstreams.local.LocalUpstream",
        help="Upstream class path", type=str)
    tornado.options.parse_config_file("config.py")
    tornado.options.parse_command_line()

    upstream_cls_name = tornado.options.options['upstream'].value()
    upstream_cls = import_class(upstream_cls_name)
    manager_cls = getattr(upstream_cls, "MANAGER_CLS", None)
    if manager_cls:
        manager = manager_cls()
    else:
        manager = None

    upstream_cls.on_load()
    tornado.options.parse_config_file("config.py")

    sys.exit(main(tornado.options.options['processes'].value(),
        tornado.options.options['bind'].value(),
        tornado.options.options['port'].value(),
        upstream_cls, manager))
