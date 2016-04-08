# -*- coding: utf-8 -*-

from __future__ import absolute_import

import json
import traceback
import logging

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


log = logging.getLogger('tornado-signal')


class TornadoSignal(Signal):

    def __init__(self, providing_args=None, name=None, serializer=None):
        super(TornadoSignal, self).__init__(providing_args)
        self.name = name

    @gen.coroutine
    def send_async(self, sender, **named):
        """
            Send signal using tornado coroutines. Run parallel.
            Used when needs wait for finishing all handlings.

            Arguments:

                sender
                    Any python object, initiator of action.

                named
                    Keyword arguments which will be passed to receivers.

            @gen.coroutine
            def foo():
                yield signals.my_signal.send_async(sender=sender, data=data)

        """

        recs = self._live_receivers(_make_id(sender))
        yield [rec(signal=self, sender=sender, **named) for rec in recs]

    def send_spawn(self, sender, **named):
        """
            Send signal using tornado spawn_callback. Run parallel.
            Used when wants to continue without waiting for handling
            signal receivers.

            Arguments:

                sender
                    Any python object, initiator of action.

                named
                    Keyword arguments which will be passed to receivers.

            def foo():
                signals.my_signal.send_spawn(sender=sender, data=data)

        """

        for receiver in self._live_receivers(_make_id(sender)):
            IOLoop.current().spawn_callback(
                receiver, signal=self, sender=sender, **named)


class RedisPubSubSignal(TornadoSignal):
    _instances = {}
    channel_prefix = 'pubsub'
    redis_publisher = None
    redis_subscriber = None
    _debug = False
    log = None

    def __init__(self, providing_args=None, name=None, serializer=None):
        """
            Signal constructor

            Arguments:

                providing_args
                    list of strings - names of arguments wich
                    will be transferred with signal

                name
                    string of latin letters - name of signal i
                    used in name of redis channel

                serializer
                    serializer module with "loads" and "dumps" methods, i
                    by default python json module

            my_signal = dispatch.RedisPubSubSignal(
                providing_args=['key'],
                name='my_signal',
                serializer=custom_json_module)
        """

        if not(self.redis_publisher and self.redis_subscriber):
            raise Exception('You must specify pub/sub clients: \n'
                            'dispatch.RedisPubSubSignal.initialize(\n'
                            '    redis_cfg={}, channel_prefix="test")\n')

        super(RedisPubSubSignal, self).__init__(providing_args)
        self.name = name
        self.serializer = serializer or json
        self._instances[name] = self

    @classmethod
    def initialize(cls, publisher=None, subscriber=None,
                   redis_cfg=None, channel_prefix=None,
                   debug=False, logger=None):
        """
            Initializer pubsub clients and options

            Arguments:

                publisher
                    redis.Client object or None,
                    client for publishing messages

                subscriber
                    tornadoredis.Client or redis.Client or None,
                    client for listen incoming messages

                redis_cfg
                    dict of config for automatic creating publisher
                    and subscriber client objects

                channel_prefix
                    str - prefix of redis channel name

                debug
                    True for debug mode (more logs)

                logger
                    custom logger
        """

        cls._debug = debug
        cls.log = logger or log

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

    @classmethod
    @gen.engine
    def listen(cls):
        """
            Start listen incoming messages.

            Call it after signals definition, else signals defined after
            running listener will be ignored. You can define signals after
            start listener if you don't want get messages from it. Or don't
            call this method if you don't want get any incoming messages.

            dispatch.RedisPubSubSignal.initialize(**kwargs)

            my_signal = dispatch.RedisPubSubSignal(
                providing_args=['key'], name='my_signal')

            dispatch.RedisPubSubSignal.listen()
        """

        cls.redis_subscriber.connect()
        _all = [cls.get_channel_name(s, '*') for s in cls._instances.keys()]
        cls._debug and cls.log.debug('LISTEN:\n\t{}'.format('\n\t'.join(_all)))
        yield gen.Task(cls.redis_subscriber.psubscribe, _all)
        cls.redis_subscriber.listen(cls.receive_from_redis)

    def serialize(self, data):
        return self.serializer.dumps(data)

    def deserialize(self, data):
        return self.serializer.loads(data)

    def send_redis(self, sender, **named):
        """
            Send signal over redis pub/sub mechanism

            Arguments:

                sender
                    JSON-serializable python object

                named
                    Keyword arguments which will be passed to receivers,
                    only JSON-serializable python objects

            signals.my_signal.send_redis(
                sender='some_sender', key='12345')
        """

        self._debug and self.log.debug('SEND TO REDIS {}:{} {}'.format(
            self.name, sender, named))
        IOLoop.current().spawn_callback(
            self.redis_publisher.publish,
            self.get_channel_name(self.name, sender),
            self.serialize(named))

    @classmethod
    def receive_from_redis(cls, message):
        cls._debug and cls.log.debug('RECEIVE FROM REDIS {}'.format(message))

        if message.kind in ('message', 'pmessage'):
            try:
                _pref, name, sender = message.channel.split(':')
                signal = cls._instances.get(name)
                if signal:
                    signal.send_spawn(sender, **signal.deserialize(message.body))
                else:
                    cls.log.warning('RECEIVE UNKNOWN SIGNAL {}'.format(name))

            except Exception as e:
                cls.log.error('RECEIVE FROM REDIS {}'.format(message))
                cls.log.error('RECEIVE CRASH: {}'.format(e))
                cls.log.error(traceback.format_exc())

        elif message.kind == 'disconnect':
            cls.log.warning('REDIS PUBSUB RECONNECT')
            cls.redis_subscriber.connect()

