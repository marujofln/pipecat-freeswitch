#
# Copyright (c) 2026, Marcelo Araujo
#
# SPDX-License-Identifier: BSD 2-Clause License
#

"""Pipecat integration for FreeSWITCH via the mod_audio_stream WebSocket module."""

from pipecat_freeswitch.control import FreeSWITCHControl
from pipecat_freeswitch.esl import ESLConnection, ESLError
from pipecat_freeswitch.serializer import FreeSWITCHFrameSerializer

__version__ = "0.1.0"

__all__ = [
    "ESLConnection",
    "ESLError",
    "FreeSWITCHControl",
    "FreeSWITCHFrameSerializer",
]
