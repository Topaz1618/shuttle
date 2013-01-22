import tornado.web
import tornado.ioloop
import functools
from sleekxmpp import ClientXMPP
import urllib2
import json


# https://github.com/whardier/jabberhooky/blob/master/jabberhooky/__main__.py
class XMPPProxyCallback(tornado.web.RequestHandler):
    def initialize(self, callback):
        self.ioloop = tornado.ioloop.IOLoop.instance()
        self.callback = callback

    def post(self):
        self.finish()
        arg_callback = functools.partial(self.callback, self.request.body)
        self.ioloop.add_callback(arg_callback)


class XMPPProxyBot(ClientXMPP):
    MESSAGE_KEYS = ['body', 'from', 'id', 'type', 'to']

    def __init__(self, jid, password, start_callback, message_callback):
        ClientXMPP.__init__(self, jid, password)
        self.add_event_handler("session_start", self.on_start)
        self.add_event_handler("message", self.on_message)
        self.start_callback = start_callback
        self.message_callback = message_callback
        # self.register_plugin('xep_0030')  # Service Discovery
        # self.register_plugin('xep_0004')  # Data Forms
        # self.register_plugin('xep_0060')  # PubSub
        # self.register_plugin('xep_0199')  # XMPP Ping

    def on_start(self, event):
        self.send_presence()
        self.get_roster()
        urllib2.urlopen(self.start_callback, self.boundjid.full)

    def on_message(self, msg):
        if msg['type'] in ['chat', 'normal']:
            extracted = dict([(x, str(msg[x])) for x in self.MESSAGE_KEYS])
            urllib2.urlopen(self.message_callback, json.dumps(extracted))
