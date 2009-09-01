apo = require('actor_post_office')

local n_sends = 0
local n_recvs = 0

function player(self_addr, name, n, max)
  while true do
    ball = apo.recv()
    n_recvs = n_recvs + 1
    print(name .. " got ball, hits " .. ball.hits)

    if ball.hits <= max then
      for i = 1, n do
        apo.send(ball.from, { from = self_addr, hits = ball.hits + 1 })
        n_sends = n_sends + 1
      end
    end
  end
end

mike_addr = apo.spawn(player, "Mike", 5, 10)
mary_addr = apo.spawn(player, "Mary", 1, 10)

apo.send(mike_addr, { from = mary_addr, hits = 1})
n_sends = n_sends + 1

print(n_sends, n_recvs)
assert(n_sends == n_recvs, n_sends, n_recvs)


