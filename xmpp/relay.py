# ugly fix for loading upstream.local.LocalUpstream
import sys
import os
sys.path.insert(0, os.path.abspath('..'))

import logging
import tornado.web
import tornado.process
from tornado.options import define, options, parse_config_file
import tornado.ioloop
import json
from upstreams.local import LocalUpstream
from integration import XMPPProxyBot, XMPPProxyCallback


class XMPPRelaySession(object):
    def __init__(self, peer, manager):
        self.peer = peer
        self.manager = manager
        self.handlers = {
            'connect':  self.do_connect,
            'send':     self.do_send,
            'close':    self.do_close,
            'ping':     self.do_ping,
        }
        self.streams = {}

    def on_request(self, req, msg):
        action = req.get('action', None)
        if action in self.handlers:
            self.handlers[action](req, msg)
        else:
            logging.warning("unknown action: %s" % action)

    def do_connect(self, req, msg):
        if any([key not in req for key in ["id", "destination",
            "address_type"]]):
            logging.warning("request missing key: %s" % (str(req)))
            return
        stream_id = str(req.get("id"))
        destination = tuple(req.get("destination"))
        address_type = int(req.get("address_type"))
        stream = LocalUpstream(destination, address_type,
            self.on_upstream_connect, self.on_upstream_error,
            self.on_upstream_data, self.on_upstream_close)
        stream.stream_id = stream_id
        self.streams[stream_id] = stream
        self.reply({
            'event':   'establishing',
            'id':       stream_id,
            })

    def do_ping(self, req, msg):
        self.reply({
            'event':   'pong',
            })

    def do_send(self, req, msg):
        if any([key not in req for key in ["id", "data"]]):
            logging.warning("request missing key: %s" % (str(req)))
            return
        stream_id = str(req.get("id"))
        data = bytes(req.get("data").decode("base64"))
        stream = self.streams.get(stream_id, None)
        if stream:
            stream.send(data)

    def do_close(self, req, msg):
        if any([key not in req for key in ["id"]]):
            logging.warning("request missing key: %s" % (str(req)))
            return
        stream_id = str(req.get("id"))
        stream = self.streams.get(stream_id, None)
        if stream:
            stream.close()
            del self.streams[stream.stream_id]

    def on_upstream_connect(self, stream):
        addr_type = stream.local_address_type()
        addr = stream.local_address()
        logging.info("connected to (%s:%d)" % stream.destination)
        self.reply({
            'event':     'connected',
            'id':        stream.stream_id,
            'address_type': addr_type,
            'address':      addr
            })

    def on_upstream_error(self, stream, no):
        self.delete_stream_id(stream.stream_id)
        logging.info("error in connection to (%s:%d): " % stream.destination + os.strerror(no))
        self.reply({
            'event':    'error',
            'id':       stream.stream_id,
            'errno':    no,
            })
        self.conn.close()

    def on_upstream_data(self, stream, data):
        self.reply({
            'event':    'data',
            'id':       stream.stream_id,
            'data':     data.encode("base64"),
            })

    def on_upstream_close(self, stream):
        self.delete_stream_id(stream.stream_id)
        self.reply({
            'event':    'closed',
            'id':       stream.stream_id,
            })

    def reply(self, r):
        body = json.dumps(r)
        self.manager.send_message(mto=self.peer, mbody=body)
        logging.info("sent reply: %s" % r['event'])

    def delete_stream_id(self, stream_id):
        try:
            del self.streams[stream_id]
        except KeyError:
            pass


class XMPPSessionManager(object):
    def __init__(self, xmpp):
        self.xmpp = xmpp
        self.sessions = {}

    def send_message(self, *args, **kwargs):
        self.xmpp.send_message(*args, **kwargs)

    def on_start(self, jid):
        self.jid = jid
        logging.info("connected with JID: %s" % self.jid)

    def on_message(self, data):
        try:
            msg = json.loads(data)
        except Exception as e:
            logging.warning("cannot parse message from xmpp \
                callback: %s, error: %s" % (data, str(e)))
            return

        try:
            req = json.loads(msg['body'])
        except Exception as e:
            logging.warning("cannot parse request: %s, error: %s\
                " % (msg['body'], str(e)))
            return

        if msg['from'] not in self.sessions:
            self.sessions[msg['from']] = XMPPRelaySession(msg['from'], self)
        self.sessions[msg['from']].on_request(req, msg)


if __name__ == '__main__':
    define("xmpp_jid", help="XMPP JID. e.g.: user@gmail.com", type=str)
    define("xmpp_password", help="XMPP Password.", type=str)
    define("xmpp_host", help="XMPP Host", type=str)
    define("xmpp_port", help="XMPP Port", type=int)
    define("callback_port", help="HTTP callback port.", type=int)

    parse_config_file("relay-config.py")

    start = "http://127.0.0.1:%d/start/" % options['callback_port'].value()
    message = "http://127.0.0.1:%d/message/" % options['callback_port'].value()
    xmpp = XMPPProxyBot(options['xmpp_jid'].value(),
        options['xmpp_password'].value(), start, message)

    manager = XMPPSessionManager(xmpp)
    app = tornado.web.Application([
        (r'/start/', XMPPProxyCallback, {"callback": manager.on_start}),
        (r'/message/', XMPPProxyCallback, {"callback": manager.on_message}),
    ])

    if xmpp.connect((options['xmpp_host'].value(),
        options['xmpp_port'].value())):
        xmpp.process(block=False)

    try:
        app.listen(options['callback_port'].value())
        ioloop = tornado.ioloop.IOLoop.instance()
        ioloop.start()
    except KeyboardInterrupt:
        ioloop.stop()
        xmpp.disconnect(wait=False)
