# -*- coding: utf-8 -*-

from __future__ import absolute_import

import json

from dispatch.dispatcher import Signal, _make_id

from tornado import gen
from tornado.ioloop import IOLoop

try:
    import redis
except ImportError:
    redis = None

try:
    import tornadoredis
except ImportError:
    tornadoredis = None


class TornadoSignal(Signal):

    def __init__(self, providing_args=None, name=None, serializer=None):
        super(TornadoSignal, self).__init__(providing_args)
        self.name = name

    @gen.coroutine
    def send_async(self, sender, **named):
        """
            Send signal using tornado coroutines. Run parallel.
        """
        recs = self._live_receivers(_make_id(sender))
        yield [rec(signal=self, sender=sender, **named) for rec in recs]

    def send_spawn(self, sender, **named):
        """
            Send signal using tornado spawn_callback. Run parallel.
        """
        for receiver in self._live_receivers(_make_id(sender)):
            IOLoop.current().spawn_callback(
                receiver, signal=self, sender=sender, **named)


class RedisPubSubSignal(TornadoSignal):
    channel_prefix = 'pubsub'
    redis_publisher = None
    redis_subscriber = None

    def __init__(self, providing_args=None, name=None, serializer=None):
        if not(self.redis_publisher and self.redis_subscriber):
            raise Exception('You must specify pub/sub clients: \n'
                            'dispatch.RedisPubSubSignal.initialize(\n'
                            '    redis_cfg={}, channel_prefix="test")\n')

        super(RedisPubSubSignal, self).__init__(providing_args)
        self.name = name
        self.serializer = serializer or json
        self.listen()

    @classmethod
    def initialize(cls, publisher=None, subscriber=None,
                   redis_cfg=None, channel_prefix=None):
        """
            Initializer pubsub clients and options
        """

        if not redis:
            raise Exception('RedisPubSubSignal required redis python lib.')

        _cfg = redis_cfg or {}
        cls.channel_prefix = channel_prefix or 'pubsub'
        cls.redis_publisher = publisher or redis.Client(**_cfg)

        if tornadoredis:
            cls.redis_subscriber = subscriber or tornadoredis.Client(**_cfg)
        else:
            cls.redis_subscriber = subscriber or redis.Client(**_cfg)

    @classmethod
    def get_channel_name(cls, name, sender):
        return '{}:{}:{}'.format(cls.channel_prefix, name, sender)

    @gen.engine
    def listen(self):
        self.redis_subscriber.connect()
        channel_name = self.get_channel_name(self.name, '*')
        yield gen.Task(self.redis_subscriber.psubscribe, channel_name)
        self.redis_subscriber.listen(self.receive_from_redis)

    def serialize(self, data):
        return self.serializer.dumps(data)

    def deserialize(self, data):
        return self.serializer.loads(data)

    def send_redis(self, sender, **named):
        """
            Send signal over redis pub/sub mechanism
        """
        channel_name = self.get_channel_name(self.name, sender)
        self.redis_publisher.publish(channel_name, self.serialize(named))

    def receive_from_redis(self, message):
        if message.kind in ('message', 'pmessage'):
            sender = message.channel.split(':')[-1]
            self.send_spawn(sender, **self.deserialize(message.body))
        elif message.kind == 'disconnect':
            self.redis_subscriber.connect()

