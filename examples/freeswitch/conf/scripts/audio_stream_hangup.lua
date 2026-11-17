-- Hang up a call whose Pipecat bot went away (hooked in lua.conf.xml).
--
-- mod_audio_stream raises mod_audio_stream::error when its WebSocket fails or drops without a
-- WebSocket CLOSE: the bot process crashed or was killed (the kernel closes the TCP connection),
-- the bot's host stopped answering pings (STREAM_HEART_BEAT), or the connection could not be opened.
-- The bot cannot hang up after its own death, so FreeSWITCH does it here; otherwise the dialplan's
-- park keeps the caller on a silent line until they give up.
--
-- To send the caller somewhere else instead (a human, a "please call back" message), replace the
-- uuid_kill with: uuid_transfer <uuid> <extension> XML <context>

local uuid = event:getHeader("Unique-ID")
if not uuid then
    return
end

freeswitch.consoleLog("WARNING", string.format(
    "[audio_stream_hangup] %s: bot connection lost (%s), hanging up\n", uuid, event:getBody() or ""))
freeswitch.API():executeString("uuid_kill " .. uuid .. " NORMAL_TEMPORARY_FAILURE")
