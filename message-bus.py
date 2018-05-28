import json
import tornado.ioloop
import tornado.web


class MessageAggregator(object):
    __slots__ = ["messages", "callback_on_update"]

    def __init__(self):
        self.messages = {}
        self.callback_on_update = None

    def push_message(self, message: json):
        unique_msg_key = "%s:%s:%s" % (message["account"], message["id"], message["data"]["date"])
        if unique_msg_key in self.messages:
            msg = self.messages[unique_msg_key]
            msg["data"]["costs"] += message["data"]["costs"]
            msg["data"]["shows"] += message["data"]["shows"]
            msg["data"]["clicks"] += message["data"]["clicks"]
        else:
            self.messages[unique_msg_key] = message

        if self.callback_on_update is not None:
            self.callback_on_update()

    def pop_message(self, channel):
        for message in filter(lambda msg: msg[:msg.find(":")] == channel, self.messages):
            return self.messages.pop(message)
        return None

    def register_callback_on_update(self, callback):
        self.callback_on_update = callback


class MessageBusApplication(tornado.web.Application):
    def __init__(self, handlers=None, default_host=None, transforms=None, **settings):
        super(MessageBusApplication, self).__init__(handlers, default_host, transforms, **settings)
        self.message_aggregator = MessageAggregator()
        self.message_aggregator.register_callback_on_update(self._resume_connections)
        self.pending_connections = []

    def _resume_connections(self):
        connection_to_release = []
        for connection in self.pending_connections:
            message = self.message_aggregator.pop_message(connection.channel)
            if message is not None:
                connection.finish(message)
                connection_to_release.append(connection)
        for conn in connection_to_release:
            self.pending_connections.remove(conn)


class PushMessageHandler(tornado.web.RequestHandler):
    @tornado.web.asynchronous
    def post(self, *args, **kwargs):
        self.application.message_aggregator.push_message(json.loads(self.request.body))
        self.finish()


class ChannelSubscribeHandler(tornado.web.RequestHandler):
    def __init__(self, application, request, **kwargs):
        super(ChannelSubscribeHandler, self).__init__(application, request, **kwargs)
        self._channel = None

    @property
    def channel(self):
        return self._channel

    @channel.setter
    def channel(self, value):
        self._channel = value

    @tornado.web.asynchronous
    def get(self, *args, **kwargs):
        self.channel, = args
        response = self.application.message_aggregator.pop_message(self.channel)
        if response is None:
            self.application.pending_connections.append(self)
        else:
            self.finish(response)


def main():
    application = MessageBusApplication([
        (r"/message", PushMessageHandler),
        (r"/subscribe/([\w]+)", ChannelSubscribeHandler)
    ])
    application.listen(8080)
    tornado.ioloop.IOLoop.current().start()


if __name__ == "__main__":
    main()
