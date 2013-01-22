import tornado.iostream
from tornado.options import define, options, parse_config_file
import socket
import functools
from upstreams.upstream import Upstream
from objectlog import LoggingEnabledObject
from integration import XMPPProxyCallback, XMPPProxyBot
import json
import uuid
import os


class XMPPManager(LoggingEnabledObject):
    def __init__(self):
        define("xmpp_jid", help="XMPP JID. e.g.: user@gmail.com", type=str)
        define("xmpp_password", help="XMPP Password.", type=str)
        define("xmpp_host", help="XMPP Host", type=str)
        define("xmpp_port", help="XMPP Port", type=int)
        define("xmpp_relay_user", help="XMPP relay user", type=str)
        define("callback_port", help="HTTP callback port.", default=15827,
            type=int)

        config_path = os.path.join(os.path.dirname(os.path.realpath(__file__)),
            "upstream-config.py")
        parse_config_file(config_path)

        self.relay_user = options['xmpp_relay_user'].value()

        super(XMPPManager, self).__init__("XMPPManager(%s)",
            options['xmpp_jid'].value())

        start = "http://127.0.0.1:%d/start/" % options['callback_port'].value()
        message = "http://127.0.0.1:%d/message/" % options['callback_port'
            ].value()
        self.xmpp = XMPPProxyBot(options['xmpp_jid'].value(),
            options['xmpp_password'].value(), start, message)

        self.app = tornado.web.Application([
            (r'/start/', XMPPProxyCallback, {"callback": self.on_start}),
            (r'/message/', XMPPProxyCallback, {"callback": self.on_message})
        ])

        if self.xmpp.connect((options['xmpp_host'].value(),
            options['xmpp_port'].value())):
            self.xmpp.process(block=False)

        self.streams = {}
        self.app.listen(options['callback_port'].value())

    def cleanup(self):
        self.xmpp.disconnect(wait=False)

    def on_start(self, resource_name):
        self.info("resource name: " + resource_name)

    def on_message(self, data):
        try:
            msg = json.loads(data)
        except Exception as e:
            self.warning("cannot parse message from xmpp \
                callback: %s, error: %s" % (data, str(e)))
            return

        try:
            reply = json.loads(msg['body'])
        except Exception as e:
            self.warning("cannot parse request: %s, error: %s\
                " % (msg['body'], str(e)))
            return
        self.on_reply(reply, msg)

    def on_reply(self, reply, msg):
        if reply['event'] == 'pong':
            pass
        else:
            stream_id = reply['id']
            stream = self.streams[stream_id]
            if reply['event'] == 'establishing':
                pass
            elif reply['event'] == 'error':
                stream.on_error(reply['errno'])
            elif reply['event'] == 'closed':
                stream.on_close()
            elif reply['event'] == 'data':
                stream.on_streaming_data(bytes(reply['data']))
            elif reply['event'] == 'connected':
                stream.local_addr = tuple(reply['address'])
                stream.on_connect()
            else:
                self.warning("unknown event: %s" % reply['event'])

    def send_request(self, req):
        serialized = json.dumps(req)
        self.xmpp.send_message(mbody=serialized, mto=self.relay_user)

    def add_stream(self, stream):
        self.streams[stream.stream_id] = stream


class XMPPUpstream(Upstream):
    MANAGER_CLS = XMPPManager

    def do_init(self):
        self.stream_id = uuid.uuid4().hex
        self.manager.add_stream(self)

    def do_connect(self):
        self.manager.add_stream(self)
        self.manager.send_request({
            'id':           self.stream_id,
            'action':       'connect',
            'destination':  self.destination,
            'address_type':    self.address_type
            })

    def local_address_type(self):
        return self.address_type

    def local_address(self):
        return self.local_addr

    def on_connect(self):
        self.debug("connected!")
        self.connection_callback(self)

    def on_error(self, errno):
        self.debug("errno: %d" % errno)
        self.error_callback(self, errno)

    def on_close(self):
        self.debug("closed")
        self.close_callback(self)

    def on_streaming_data(self, data, finished=False):
        if len(data):
            self.debug("received %d bytes of data." % len(data))
            self.streaming_callback(self, data)

    def do_send(self, data):
        self.manager.send_request({
            'id':           self.stream_id,
            'action':       'send',
            'data':         data
            })

    def do_close(self):
        self.manager.send_request({
            'id':           self.stream_id,
            'action':       'close',
            })

if __name__ == '__main__':
    define("xmpp_jid", help="XMPP JID. e.g.: user@gmail.com", type=str)
    define("xmpp_password", help="XMPP Password.", type=str)
    define("xmpp_host", help="XMPP Host", type=str)
    define("xmpp_port", help="XMPP Port", type=int)
    define("callback_port", help="HTTP callback port.", type=int)
