-- actor_post_office
--
-- simple erlang-like, concurrent-lua-like system,
-- enabling cooperative actor-like application programming.
--
-- for local process only (not distributed), single main thread,
-- based on lua coroutines, with a trampoline-based design.

----------------------------------------

function actor_post_office_create()

local last_addr = 0

-- Map actor addresses to actor coroutines and vice-versa.

local map_addr_to_coro = {} -- table, key'ed by addr.
local map_coro_to_addr = {} -- table, key'ed by coro.
local map_coro_to_data = {} -- table, key'ed by coro, for user data.

local map_addr_to_watchers = {} -- table, key'ed by target addr, value is a
                                -- table, key'ed by watcher addr.

local envelopes = {}

----------------------------------------

local main_todos = {} -- array of funcs/closures, to be run on main thread.

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
end

----------------------------------------

local function next_address()
  local curr_addr

  repeat
    last_addr = last_addr + 1
    curr_addr = tostring(last_addr)
  until map_addr_to_coro[curr_addr] == nil

  return curr_addr
end

local function coroutine_address(coro)
  if coro then
    return map_coro_to_addr[coro]
  end

  return nil
end

local function self_address()
  return coroutine_address(coroutine.running())
end

----------------------------------------

local function unregister(addr)
  local coro = map_addr_to_coro[addr]
  if coro then
    map_addr_to_coro[addr] = nil
    map_coro_to_addr[coro] = nil
    map_coro_to_data[coro] = nil
    map_addr_to_watchers[addr] = nil
  end
end

local function register(coro)
  unregister(map_coro_to_addr[coro])

  local curr_addr = next_address()

  map_addr_to_coro[curr_addr] = coro
  map_coro_to_addr[coro] = curr_addr

  return curr_addr
end

local function is_registered(addr)
  return map_addr_to_coro[addr] ~= nil
end

----------------------------------------

local function user_data()
  local coro = coroutine.running()

  local d = map_coro_to_data[coro]
  if not d then
    d = {}
    map_coro_to_data[coro] = d
  end

  return d
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
local function send_msg(dest_addr, dest_msg, track_addr, track_msg)
  table.insert(envelopes, { dest_addr  = dest_addr,
                            dest_msg   = dest_msg,
                            track_addr = track_addr,
                            track_msg  = track_msg})
end

----------------------------------------

local function finish(child_addr)
  local watchers = map_addr_to_watchers[child_addr]

  unregister(child_addr)

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
  if envelope then
    local coro = map_addr_to_coro[envelope.dest_addr]
    if coro then
      if not resume(coro, unpack(envelope.dest_msg)) then
        finish(envelope.dest_addr)
      end
    else
      -- The destination coro is gone, probably finished already,
      -- so send the tracking address a notification message.
      --
      if envelope.track_addr then
        send_msg(envelope.track_addr, envelope.track_msg)
      end
    end

    return true
  end

  return false
end

----------------------------------------

local function step()
  -- Must be invoked on main thread.
  run_main_todos()

  return deliver_envelope(table.remove(envelopes, 1))
end

local function loop_until_empty(force)
  -- Check first if we're the main thread.
  if (coroutine.running() == nil) or force then
    local go = true
    while go do
      go = step()
    end
  end
end

local function loop()
  while true do
    loop_until_empty()
  end
end

----------------------------------------

-- Asynchronous send of variable args as a message.
--
local function send_later(dest_addr, ...)
  send_msg(dest_addr, arg)
end

-- Asynchronous send of variable args as a message.
--
-- Unlike send_later(), a send() might opportunistically,
-- process the message immediately before returning.
--
local function send(dest_addr, ...)
  if dest_addr then
    send_msg(dest_addr, arg)
  end

  loop_until_empty()
end

-- Asynchronous send of variable args as a message, similar to send(),
-- except a tracking address and message can be supplied.  The
-- tracking address will be notified with the track_msg if there are
-- problems sending the message to the dest_addr, such as if the
-- destination address does not represent a live actor.
--
local function send_track(dest_addr, track_addr, track_msg, ...)
  if dest_addr then
    send_msg(dest_addr, arg, track_addr, track_msg)
  end

  loop_until_empty()
end

local function recv()
  if coroutine.running() then
    return coroutine.yield()
  end

  return nil
end

----------------------------------------

local function spawn_with(spawner, f, ...)
  local child_coro = nil
  local child_addr = nil
  local child_arg = arg
  local child_fun =
    function()
      -- TODO: Do we need xpcall around f()?
      --
      f(child_addr, unpack(child_arg))

      finish(child_addr)
    end

  child_coro = spawner(child_fun)
  child_addr = register(child_coro)

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
  return spawn_with(coroutine.create, f, ...)
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
  watcher_arg  = arg

  if target_addr and watcher_addr then
    local watchers = map_addr_to_watchers[target_addr]
    if not watchers then
      watchers = {}
      map_addr_to_watchers[target_addr] = watchers
    end

    local watcher_args = watchers[watcher_addr]
    if not watcher_args then
      watcher_args = {}
      watchers[watcher_addr] = watcher_args
    end
    watcher_args[#watcher_args + 1] = watcher_arg
  end
end

-- The unwatch() is not quite symmetric with watch(), in that
-- unwatch() clears the entire watcher_args list for a watcher
-- address.
--
local function unwatch(target_addr, watcher_addr)
  watcher_addr = watcher_addr or self_address()

  if target_addr and watcher_addr then
    local watchers = map_addr_to_watchers[target_addr]
    if watchers and
       watchers[watcher_addr] then
      watchers[watcher_addr] = nil
    end
  end
end

----------------------------------------

return {
  recv       = recv,
  send       = send,
  send_later = send_later,
  send_track = send_track,
  step       = step,
  spawn      = spawn,
  spawn_with = spawn_with,
  user_data  = user_data,
  watch      = watch,
  unwatch    = unwatch,
  register   = register,
  unregister = unregister,
  is_registered     = is_registered,
  coroutine_address = coroutine_address,
  self_address      = self_address,
  loop_until_empty  = loop_until_empty
}

end

----------------------------------------

return actor_post_office_create()
