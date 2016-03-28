"""Multi-consumer multi-producer dispatching mechanism

Originally based on pydispatch (BSD) http://pypi.python.org/pypi/PyDispatcher/2.0.1
See license.txt for original license.

Heavily modified for Django's purposes.
"""

from dispatch.dispatcher import Signal

try:
    from dispatch.tornado import TornadoSignal, RedisPubSubSignal
except ImportError:
    pass


def receiver(signal, **kwargs):
    """
        A decorator for connecting receivers to signals. Used by passing in the
        signal (or list of signals) and keyword arguments to connect::

            @receiver(post_save, sender=MyModel)
            def signal_receiver(sender, **kwargs):
                ...

            @receiver([post_save, post_delete], sender=MyModel)
            def signals_receiver(sender, **kwargs):
            ...

    """
    def _decorator(func):
        if isinstance(signal, (list, tuple)):
            for s in signal:
                s.connect(func, **kwargs)
        else:
            signal.connect(func, **kwargs)
        return func
    return _decorator
