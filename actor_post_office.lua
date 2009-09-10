-- actor_post_office
--
-- simple erlang-like, concurrent-lua-like system,
-- enabling cooperative actor-like application programming.
--
-- for local process only (not distributed), single main thread,
-- based on lua coroutines, with a trampoline-based design.

----------------------------------------

function actor_post_office_create()

local function create_mbox(addr, coro)
  return {
    addr = addr,
    coro = coro,

    -- data     = nil, -- User data for this mbox.
    -- watchers = nil, -- Array of watcher addresses.
    -- filter   = nil  -- A filter function passed in during recv()
  }
end

----------------------------------------

local last_addr = 0

-- Map actor addresses to actor coroutines and vice-versa.

local map_addr_to_mbox = {} -- table, key'ed by addr.
local map_coro_to_addr = {} -- table, key'ed by coro.

local envelopes = {}

----------------------------------------

local main_todos = {} -- Array of funcs/closures, to be run on main thread.

local function run_main_todos(force)
  -- Check first if we're the main thread.
  if (coroutine.running() == nil) or force then
    local todo = nil
    repeat
      todo = table.remove(main_todos, 1)
      if todo then
        todo()
      end
    until todo == nil
  end

  return true
end

----------------------------------------

local function next_address()
  local curr_addr

  repeat
    last_addr = last_addr + 1
    curr_addr = tostring(last_addr)
  until map_addr_to_mbox[curr_addr] == nil

  return curr_addr
end

local function coroutine_address(coro)
  if coro then
    return map_coro_to_addr[coro]
  end

  return nil
end

local function address_coroutine(addr)
  if addr then
    local mbox = map_addr_to_mbox[addr]
    if mbox then
      return mbox.coro
    end
  end

  return nil
end

local function self_address()
  return coroutine_address(coroutine.running())
end

----------------------------------------

local function unregister(addr)
  if addr then
    local mbox = map_addr_to_mbox[addr]
    if mbox then
      map_addr_to_mbox[addr] = nil
      map_coro_to_addr[mbox.coro] = nil
    end
  end
end

local function register(coro, opt_suffix)
  unregister(map_coro_to_addr[coro])

  local addr = next_address()

  if opt_suffix then
    addr = addr .. "." .. opt_suffix
  end

  map_addr_to_mbox[addr] = create_mbox(addr, coro)
  map_coro_to_addr[coro] = addr

  return addr
end

local function is_registered(addr)
  return map_addr_to_mbox[addr] ~= nil
end

----------------------------------------

local function user_data()
  local addr = self_address()
  if addr then
    local mbox = map_addr_to_mbox[addr]
    if mbox then
      local data = mbox.data
      if not data then
        data = {}
        mbox.data = data
      end

      return data
    end
  end

  return nil
end

----------------------------------------

local function resume(coro, ...)
  -- TODO: Do we need xpcall around resume()?
  --
  if coro and coroutine.status(coro) ~= 'dead' then
    local ok = coroutine.resume(coro, ...)
    if not ok then
      print(debug.traceback(coro))
    end

    return ok
  end

  return false
end

----------------------------------------

-- Lowest-level asynchronous send of a message.
--
local function send_msg(dest_addr, dest_msg, track_addr, track_args)
  table.insert(envelopes, { dest_addr  = dest_addr,
                            dest_msg   = dest_msg,
                            track_addr = track_addr,
                            track_args = track_args })
end

----------------------------------------

local function finish(addr)
  local watchers = nil

  local mbox = map_addr_to_mbox[addr]
  if mbox then
    watchers = mbox.watchers
  end

  unregister(addr)

  -- Notify watchers.
  --
  if watchers then
    for watcher_addr, watcher_args in pairs(watchers) do
      if watcher_addr then
        for i = 1, #watcher_args do
          send_msg(watcher_addr, watcher_args[i])
        end
      end
    end
  end
end

----------------------------------------

local function deliver_envelope(envelope)
  -- Must be invoked on main thread.
  --
  if envelope and
     envelope.dest_addr then
    local mbox = map_addr_to_mbox[envelope.dest_addr]
    if mbox then
      local dest_msg = envelope.dest_msg or {}

      if mbox.filter and not mbox.filter(unpack(dest_msg)) then
        -- Tell our caller to re-send/re-queue the envelope.
        --
        return envelope
      end

      -- Since the filter, if any, accepted the message,
      -- clear it out otherwise other messages might get
      -- inadvertently filtered.
      --
      mbox.filter = nil

      if not resume(mbox.coro, unpack(dest_msg)) then
        finish(envelope.dest_addr)
      end
    else
      -- The destination mbox/coro is gone, probably finished already,
      -- so send the tracking address a notification message.
      --
      -- We're careful here that there's either a track notification
      -- or a watcher notification (via finish() above), but not both.
      --
      if envelope.track_addr then
        send_msg(envelope.track_addr, envelope.track_args)
      end
    end
  end
end

----------------------------------------

-- Process all envelopes, requeuing any envelopes that did not
-- pass their mbox.filter and which need resending.
--
local function loop_until_empty(force)
  -- Check first if we're the main thread.
  --
  if (coroutine.running() == nil) or force then
    local delivered = 0

    repeat
      delivered = 0

      local resends = {}

      while run_main_todos() and
            (#envelopes > 0) do
        local resend = deliver_envelope(table.remove(envelopes, 1))
        if resend then
          resends[#resends + 1] = resend
        else
          delivered = delivered + 1
        end
      end

      assert(#envelopes <= 0)

      envelopes = resends
    until (#envelopes <= 0 or delivered <= 0)
  end
end

----------------------------------------

local function loop()
  while true do
    loop_until_empty()
  end
end

----------------------------------------

-- Asynchronous send of variable args as a message.
--
local function send_later(dest_addr, ...)
  send_msg(dest_addr, { ... })
end

-- Asynchronous send of variable args as a message.
--
-- Unlike send_later(), a send() might opportunistically,
-- process the message immediately before returning.
--
local function send(dest_addr, ...)
  if dest_addr then
    send_msg(dest_addr, { ... })
  end

  loop_until_empty()
end

-- Asynchronous send of variable args as a message, similar to send(),
-- except a tracking address and message can be supplied.  The
-- tracking address will be notified with the unpacked track_args if
-- there are problems sending the message to the dest_addr, such as if
-- the destination address does not represent a live actor.
--
local function send_track(dest_addr, track_addr, track_args, ...)
  if dest_addr then
    send_msg(dest_addr, { ... }, track_addr, track_args)
  end

  loop_until_empty()
end

-- Receive a message (via multi-return-values).
--
-- An optional opt_filter(...) function can be supplied so that the
-- actor only accepts certain messages, when the opt_filter(...)
-- returns true.
--
local function recv(opt_filter)
  local coro = coroutine.running()
  if coro then
    -- The opt_filter might be nil, which is fine.
    --
    map_addr_to_mbox[coroutine_address(coro)].filter = opt_filter

    return coroutine.yield()
  end

  return nil
end

----------------------------------------

local function spawn_with(spawner, f, suffix, ...)
  local child_coro = nil
  local child_addr = nil
  local child_arg = { ... }
  local child_fun =
    function()
      -- TODO: Do we need xpcall around f()?
      --
      f(child_addr, unpack(child_arg))

      finish(child_addr)
    end

  child_coro = spawner(child_fun)
  child_addr = register(child_coro, suffix)

  table.insert(main_todos,
    function()
      if not resume(child_coro) then
        finish(child_addr)
      end
    end)

  run_main_todos()

  return child_addr
end

local function spawn(f, ...)
  return spawn_with(coroutine.create, f, nil, ...)
end

local function spawn_name(f, name, ...)
  return spawn_with(coroutine.create, f, name, ...)
end

----------------------------------------

-- Registers a watcher actor to a target actor address.  A single
-- watcher actor can register multiple times on a target actor with
-- different watcher_arg's.  When then target actor dies, the watcher
-- will be notified multiple times via a sent message, once for each
-- call to the original watch().
--
-- A call to the related unwatch() function clears all the
-- registrations for a watcher actor on a target actor.
--
local function watch(target_addr, watcher_addr, ...)
  watcher_addr = watcher_addr or self_address()
  watcher_arg  = { ... }

  if target_addr and watcher_addr then
    local mbox = map_addr_to_mbox[target_addr]
    if mbox then
      local watchers = mbox.watchers
      if not watchers then
        watchers = {}
        mbox.watchers = watchers
      end

      local watcher_args = watchers[watcher_addr]
      if not watcher_args then
        watcher_args = {}
        watchers[watcher_addr] = watcher_args
      end

      watcher_args[#watcher_args + 1] = watcher_arg
    end
  end
end

-- The unwatch() is not symmetric with watch(), in that unwatch()
-- clears the entire watcher_args list for a watcher address.  That
-- is, multiple calls to watch() for a watcher_addr, will be cleared
-- out by a single call to unwatch().
--
local function unwatch(target_addr, watcher_addr)
  watcher_addr = watcher_addr or self_address()

  if target_addr and watcher_addr then
    local mbox = map_addr_to_mbox[target_addr]
    if mbox then
      local watchers = mbox.watchers
      if watchers and
         watchers[watcher_addr] then
        watchers[watcher_addr] = nil
      end
    end
  end
end

----------------------------------------

return {
  recv       = recv,
  send       = send,
  send_later = send_later,
  send_track = send_track,
  spawn      = spawn,
  spawn_name = spawn_name,
  spawn_with = spawn_with,
  user_data  = user_data,
  watch      = watch,
  unwatch    = unwatch,

  --------------------------------

  register          = register,
  unregister        = unregister,
  is_registered     = is_registered,

  --------------------------------

  coroutine_address = coroutine_address,
  address_coroutine = address_coroutine,
  self_address      = self_address,

  --------------------------------

  loop_until_empty = loop_until_empty
}

end

----------------------------------------

return actor_post_office_create()
