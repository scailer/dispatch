from distutils.core import setup

setup(
    name = 'tornado-dispatcher',
    maintainer = 'Vlasov Dmitriy',
    maintainer_email = 'scailer@veles.biz',
    description = ('A library for event-driven programmig based on '
                   'Olivier Verdier code extended with tornado corouting and '
                   'redis pubsub mechanism.'),
    packages = [
        'dispatch',
        'dispatch.tests',
    ],
    version = '1.2.0',
    url = 'https://github.com/scailer/dispatch',
    classifiers = [
        "Development Status :: 4 - Beta",
        "Operating System :: OS Independent",
        "Programming Language :: Python",
        "License :: OSI Approved :: BSD License",
        "Intended Audience :: Developers",
        "Topic :: Software Development :: Libraries :: Python Modules",
    ],
)
