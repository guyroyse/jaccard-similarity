const Redis = require("ioredis")

const SCULLY_KEY = "ufo:states:scully"
const MULDER_KEY = "ufo:states:mulder"
const INTERSECTION_KEY = "ufo:states:intersection"
const UNION_KEY = "ufo:states:union"

const SCULLY_STATES = [ 
  "California",
  "Nevada",
  "Oregon",
  "Wyoming",
  "New Mexico",
  "Ohio" ]

const MULDER_STATES = [
  "Florida",
  "Kansas",
  "South Carolina",
  "West Virginia",
  "New Mexico",
  "Ohio" ]

async function main() {

  const redis = new Redis("redis://:foobared@localhost:6379/0")

  // make sure we're working with a clean slate
  await redis.unlink(SCULLY_KEY)
  await redis.unlink(MULDER_KEY)

  // create the sets
  await redis.sadd(SCULLY_KEY, ...SCULLY_STATES)
  await redis.sadd(MULDER_KEY, ...MULDER_STATES)

  // calculate all the ways
  let results = {
    lastMileInJavaScript: await lastMileInJavaScript(redis),
    lastMileInLua: await lastMileInLua(redis),
    unsafeButElegantInLua: await unsafeButElegantInLua(redis),
    safeButMessyInLua: await safeButMessyInLua(redis)
  }

  console.log(results)

  // quit so all our promises resolve
  await redis.quit()

}

async function lastMileInJavaScript(redis) {

  // store the intermediate sets
  await redis.sinterstore(INTERSECTION_KEY, SCULLY_KEY, MULDER_KEY)
  await redis.sunionstore(UNION_KEY, SCULLY_KEY, MULDER_KEY)

  // get the cardinality
  let intersectionCardinality = await redis.scard(INTERSECTION_KEY)
  let unionCardinality = await redis.scard(UNION_KEY)

  // return the similarity
  return intersectionCardinality / unionCardinality
}

async function lastMileInLua(redis) {

  // store the intermediate sets
  await redis.sinterstore(INTERSECTION_KEY, SCULLY_KEY, MULDER_KEY)
  await redis.sunionstore(UNION_KEY, SCULLY_KEY, MULDER_KEY)

  // use Lua to get the cardinality of the two sets, divide, and return
  let lua = `
    local inter_card = redis.pcall('scard', KEYS[1])
    local union_card = redis.pcall('scard', KEYS[2])
    local similarity = inter_card / union_card
    return tostring(similarity)`

  // invoke the Lua
  return Number(await executeLua(redis, lua, [INTERSECTION_KEY, UNION_KEY]))
}

async function unsafeButElegantInLua(redis) {

  // NOTE: There are no intermediate sets. Lua stores those instead. This
  // is elegant but only works with smaller sets. With larger sets, this
  // will likely exceed the amount of memory allowed Lua.

  // use Lua to get the intersection and union of the two sets in memory,
  // get the cardinality of each, divide, and return
  let lua = `
    local inter = redis.pcall('sinter', KEYS[1], KEYS[2])
    local union = redis.pcall('sunion', KEYS[1], KEYS[2])
    local similarity = #inter / #union
    return tostring(similarity)`

  // invoke the Lua
  return Number(await executeLua(redis, lua, [SCULLY_KEY, MULDER_KEY]))
}

async function safeButMessyInLua(redis) {

  // NOTE: There are intermediate sets that are created and deleted by
  // Lua. The key names for these intermediate sets must be passed in
  // to the script. This is a small price to pay for the benefit of being
  // able to handle very large sets.

  // use Lua to create the intersection and union in Redis, get the
  // cardinality of each, divide, clean up the intermediate sets and return
  let lua = `
    redis.pcall('sinterstore', KEYS[3], KEYS[1], KEYS[2])
    redis.pcall('sunionstore', KEYS[4], KEYS[1], KEYS[2])
    local inter_card = redis.pcall('scard', KEYS[3])
    local union_card = redis.pcall('scard', KEYS[4])
    redis.pcall('unlink', KEYS[3], KEYS[4])
    local similarity = inter_card / union_card
    return tostring(similarity)`

  // invoke the Lua
  return Number(await executeLua(redis, lua, [SCULLY_KEY, MULDER_KEY, INTERSECTION_KEY, UNION_KEY]))
}

async function executeLua(redis, script, keys) {
  return await redis.eval(script, keys.length, ...keys)
}

main()
