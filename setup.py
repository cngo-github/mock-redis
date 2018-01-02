import os

import setuptools

module_path = os.path.join(os.path.dirname(__file__), 'pytest-mockredis', '__init__.py')
version_line = [line for line in open(module_path)
                if line.startswith('__version__')][0]

__version__ = version_line.split('__version__ = ')[-1][1:][:-2]

setuptools.setup(
    name='pytest-mockredis',
    version=__version__,
    url='https://github.com/cngo-github/mock-redis',

    author='Chuong Ngo',
    author_email='cngo.github@gmail.com',

    description='An in-memory mock of a Redis server that runs in a separate thread. This is to be used for unit-tests that require a Redis database.',
    long_description=open('README.md').read(),
    packages=setuptools.find_packages(),

    zip_safe=False,
    platforms='any',

    install_requires=['hiredis>=0.2.0'],

    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Environment :: Web Environment',
        'Intended Audience :: Developers',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
    ],
)
