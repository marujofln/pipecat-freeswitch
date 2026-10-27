#
# Copyright (c) 2026, Marcelo Araujo
#
# SPDX-License-Identifier: BSD 2-Clause License
#

import pytest

from fake_esl import FakeESLServer


@pytest.fixture
async def esl_server():
    server = FakeESLServer()
    await server.start()
    yield server
    await server.stop()
