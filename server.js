const Redis = require("ioredis")

async function main() {

  const redis = new Redis("redis://:foobared@localhost:6379/0")

  await redis.del("ufo:states:sculder")
  await redis.del("ufo:states:mully")

  await redis.sadd("ufo:states:sculder", "California", "Nevada",
    "Oregon","Wyoming", "New Mexico", "Ohio")

  await redis.sadd("ufo:states:mully", "Florida", "Kansas",
    "South Carolina", "West Virginia", "New Mexico", "Ohio")

  await redis.sinterstore(`ufo:states:intersection:${Math.random()}`,
    "ufo:states:sculder", "ufo:states:mully")

  await redis.sunionstore("ufo:states:union",
    "ufo:states:sculder", "ufo:states:mully")

  let interCard = await redis.scard("ufo:states:intersection")
  let unionCard = await redis.scard("ufo:states:union")

  let lua = `
    local inter_card = redis.pcall('scard', KEYS[1])
    local union_card = redis.pcall('scard', KEYS[2])
    local similarity = inter_card / union_card
    return tostring(similarity)`

  let lua2 = `
    local inter = redis.pcall('sinter', KEYS[1], KEYS[2])
    local union = redis.pcall('sunion', KEYS[1], KEYS[2])
    local similarity = #inter / #union
    return tostring(similarity)`

  let keys = ['ufo:states:intersection', 'ufo:states:union']

  let similarity = await redis.eval(lua2, keys.length, ...keys)

  await redis.quit()

  let result = `${interCard} / ${unionCard} = ${similarity}`

  console.log(result)

}

main()
