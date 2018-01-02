#!/usr/bin/env python
# -*- coding: utf8 -*-
#
# Copyright (C) 2011 Sebastian Rahlf <basti at redtoad dot de>
#
# This program is release under the MIT license. You can find the full text of
# the license in the LICENSE file.

import pytest

@pytest.fixture
def redisserver(request):
    from pytest_mockredis.server import Redis
    server = Redis()
    server.start()
    request.addfinalizer(server.stop)
    return server
