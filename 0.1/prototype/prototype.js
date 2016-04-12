'use strict'
const http = require('http')
const got = require('got')
const chalk = require('chalk')

class RPC {
  constructor (id) {
    this._id = '' + id
    this._next_id = 1
    this._readyp = new Map()
  }

  get id () {
    return this._id
  }

  netdelay () {
    return 200 * (Math.random() + 0.5)
  }

  server (handler) {
    const color = ['red', 'yellow', 'green', 'cyan', 'blue', 'magenta'][this.id % 6]
    http.createServer((req, res) => {
      let parts = []
      let id = '#' + this._next_id++
      req.on('data', part => parts.push(part))
      req.on('end', () => {
        let name = require('url').parse(req.url).pathname.replace(/\//g, '')
        let json, outdata
        new Promise((resolve, reject) => {
          let text = Buffer.concat(parts).toString('utf8')
          json = JSON.parse(text)
          setTimeout(() => {
            if (!(json && 'FROM' in json)) console.log('\n')
            console.log(chalk[color]('%d %d IN<- %s %s %s %s'), Date.now(), this.id, json && json.FROM, id, name, JSON.stringify(json, null, 2))
            resolve(json)
          }, this.netdelay())
        }).then(json => handler(name, json)).then(
          result => [200, new Buffer(JSON.stringify(outdata = result, null, 2) + '\n', 'utf8')],
          error => [500, new Buffer(JSON.stringify(outdata = (error instanceof Error ? error.toString() : error), null, 2) + '\n', 'utf8')]
        ).then(code_buf => {
          // suppress return line for one-way calls
          if (outdata !== undefined || !(json && 'CLOCK' in json)) {
            console.log(chalk[color].bold('%d %d IN-> %s %s %s'), Date.now(), this.id, json && json.FROM, id, JSON.stringify(outdata, null, 2))
          }
          setTimeout(() => {
            res.writeHead(code_buf[0], {
              'Content-Length': code_buf[1].length,
              'Content-Type': 'application/json'
            })
            res.end(code_buf[1])
          }, this.netdelay())
        })
      })
    }).listen(this._id)
  }

  call (dest, name, args) {
    // let id = '#' + this._next_id++
    args.FROM = this.id
    args.CLOCK = Date.now()
    // console.log(chalk.black.bold('%s %s OU-> %s %s %s %s'), Date.now(), this.id, dest, id, name, JSON.stringify(args, null, 2))
    let prom = got.post(`http://localhost:${dest}/${name}`,
      { body: JSON.stringify(args, null, 2), json: true }).then(response => response.body, err => { throw err.response.body })

    // prom.then(
    //   result => console.log(chalk.black.bold('%s %s OU<- %s %s %s'), Date.now(), this.id, dest, id, JSON.stringify(result, null, 2)),
    //   error => console.log(chalk.black.bold('%s %s OU<- %s %s %s'), Date.now(), this.id, dest, id, error.message)
    // )

    return prom
  }

  qcall (dest, name, args) {
    let ready = this._readyp.get(dest) || Promise.resolve(null)
    let { resolver: clearNext, promise: newReady } = promiseAndResolver()
    this._readyp.set(dest, newReady)

    return ready.then(() => {
      let done = this.call(dest, name, args)
      done.catch(() => 0).then(() => {
        clearNext()
        if (this._readyp.get(dest) === newReady) {
          this._readyp.delete(dest)
        }
      })
      return done
    })
  }
}

function mapSet (map, key, val) {
  if (val === undefined) {
    map.delete(key)
  } else {
    map.set(key, val)
  }
}

function deepEqual (a, b) {
  return JSON.stringify(a) === JSON.stringify(b)
}

function promiseAndResolver () {
  let resolver
  let promise = new Promise((resolve, reject) => resolver = resolve)
  return { promise, resolver }
}

function waitUntil (now) {
  let { promise, resolver } = promiseAndResolver()

  function check () {
    let diff = now - Date.now()
    if (diff <= 0) {
      resolver(null)
    } else {
      setTimeout(check, diff)
    }
  }
  check()
  return promise
}

class Binding {
  // our data for this demo is the simplest possible LWW key-value store
  lubData (dkey, value1, value2) {
    if (value1 === undefined) return value2
    if (value2 === undefined) return value1
    // TODO: tack on an incrementing counter and host ID so that we never see "honest" timestamp collisions
    return (value1.v > value2.v) || (value1.v === value2.v && value1.d > value2.d) ? value1 : value2
  }

  ikMatches (ikey, entk, entv) {
    let kparts = entk.split(',')
    let iarg = ikey.substring(1)
    switch (ikey[0]) {
      case 'k':
        return entk === iarg
      case 't':
        return kparts[0] === iarg
      case 'e':
        return `${kparts[0]},${kparts[1]}` === iarg
      case 'r':
        return `${kparts[0]},${kparts[2]},${entv.d}` === iarg
      case 'p':
        return (kparts[1] + '.').startsWith(iarg + '.') || (kparts[2][0] === '@' && (kparts[2].substring(1) + '.').startsWith(iarg + '.'))
      case 'a':
        return true
    }
  }

  ikFallback (narrow) {
    const plist = (text) => {
      let out = []
      let tmp = []
      for (let part of text.split('.')) {
        if (tmp.length) out.push('p' + tmp.join('.'))
        tmp.push(part)
      }
      return out
    }

    switch (narrow[0]) {
      case 'k':
        let kparts = narrow.substring(1).split(',')
        return ['a', `t${kparts[0]}`, `e${kparts[0]},${kparts[1]}`, ...plist(kparts[1])]
      case 't': return ['a']
      case 'e':
        let eparts = narrow.substring(1).split(',')
        return ['a', `t${eparts[0]}`, ...plist(eparts[1])]
      case 'r': return [`t${narrow.substring(1)}`, 'a']
      case 'p': return [...plist(narrow.substring(1)), 'a']
      default: return []
    }
  }

  // index kfoo => data for key foo
  // index dbar => all data with value=bar
  // index a => EVERYTHING
  calculateIndex (state, ikey) {
    let need = new Set()
    let value = null

    const fetch_super = (iks) => {
      if (state.upId === null) {
        return []
      } else if (state._upIndices.has(iks)) {
        return state._upIndices.get(iks)
      } else {
        for (let fb of this.ikFallback(iks)) {
          if (state._upIndices.has(fb)) {
            return state._upIndices.get(fb).filter(kv => this.ikMatches(iks, kv[0], kv[1]))
          }
        }

        // TODO: for k-indices, we can find the answer in _any_ index
        need.add(ikey)
        return []
      }
    }

    // console.log(state.upId, state._upIndices, ikey, superval)
    let superval = fetch_super(ikey)
    superval = new Map(superval)
    // a change in the current _data could
    // * add to the mapping
    for (let ent of state._data) {
      if (superval.has(ent[0])) {
        // a matching key could delete from the index.
        // since it's in the index, we know this is the most recent upstream version.
        let upd = this.lubData(ent[0], ent[1], superval.get(ent[0]))
        // upd is now the true up to date value; drop if it's no longer a match, or update
        mapSet(superval, ent[0], this.ikMatches(ikey, ent[0], upd) ? upd : undefined)
      } else {
        // a change to something not in the map could require adding it.
        // only germane if the value matches.
        if (this.ikMatches(ikey, ent[0], ent[1])) {
          // might still be stale
          let updata = fetch_super('k' + ent[0])
          let upd = this.lubData(ent[0], ent[1], updata[0])
          if (this.ikMatches(ikey, ent[0], upd)) {
            // genuine insert
            superval.set(ent[0], upd)
          }
        }
      }
    }
    value = Array.from(superval)
    // console.log(need, value)

    if (need.size > 0) {
      value = undefined
    }
    return { need, value }
  }
}

class State {
  constructor (rpc) {
    this._rpc = rpc
    this._binding = new Binding()

    // # Last epoch number
    this._epoch = 1
    this._epochTimer = 0

    // # Upstream control
    // ID of upstream, if any
    this._upId = undefined
    let { promise: upPromise, resolver: upResolver } = promiseAndResolver()
    this._upIdPromise = upPromise
    this._upIdResolver = upResolver
    this._openArc = undefined
    this._nextArc = 1

    // Last upstream epoch which is known to us
    this._upEpoch = 0
    // Upstream-exposed index values, map from name to (opaque)
    this._upIndices = new Map()

    // # Causal state management
    // For simplicity we maintain a fiction that data keys and index keys are independent.
    // For the prototype much of the complexity can be shoveled off to the data/index mapping.
    //
    // Map from data keys to (opaque) values
    this._data = new Map()
    // Map from data keys to epochs, so that they can be removed when the upstream catches up
    this._dataEpoch = new Map()

    // Incoming updates from upstream
    // { containsTo: 5, epoch: 12, index: [{"key": 5}, {"key": 6, "value": 2}] }
    this._upQueue = []
    // Incoming updates from downstream
    // { id: 1, epoch: 17, data: [{"key": "A", "value": 1}] }
    this._downQueue = []
    // { upId: 5, upEpoch: 99, data: [...] }
    this._sibQueue = []
    this._injectQueue = []

    // Map of index keys to opaque values
    this._myIndices = new Map()

    // map of IDs to { id: 15, want: Set(...) }
    this._downstreams = new Map()
    this._siblings = new Map()
    this._nextSubId = 1
    this._subCallbacks = new Map()

    this._barrierNextId = 1
    this._barrierQueue = []
    this._barrierCallbacks = new Map()

    // clocks of ancestor and sibling systems which are included in our cut
    this._ancestorClocks = new Map()
    // [{ system, clock, target }]
    this._ancestorTriggers = []
    this._rsiblings = new Map()
  }

  get upId () {
    if (this._upId === undefined) throw new Error('_upId not yet set')
    return this._upId
  }

  newEpoch () {
    let datas_changed = new Set()
    let upindex_changed = new Set()
    let myindex_changed = new Set()
    let down_maxindex = new Map()
    let clocks_changed = []

    let new_epoch = ++this._epoch

    const copyClocks = (list) => {
      for (let entry of list) {
        if (entry.clock > (this._ancestorClocks.get(entry.system) || 0)) {
          this._ancestorClocks.set(entry.system, entry.clock)
          clocks_changed.push(entry)
        }
      }
    }

    copyClocks([ { system: this._rpc.id, clock: new_epoch } ])

    // incorporate index changes from above
    for (let upo of this._upQueue.splice(0)) {
      copyClocks(upo.clocks)
      for (let ent of this._dataEpoch) {
        if (ent[1] <= upo.containsTo) {
          this._dataEpoch.delete(ent[0])
          this._data.delete(ent[0])
        }
      }

      for (let ixent of upo.index) {
        let prev = this._upIndices.get(ixent.key)
        if (!deepEqual(prev, ixent.value)) {
          upindex_changed.add(ixent.key)
          mapSet(this._upIndices, ixent.key, ixent.value)
        }
      }

      this._upEpoch = upo.epoch
    }

    const copyData = (data) => {
      for (let daent of data) {
        let prev = this._data.get(daent.key)
        let next = this._binding.lubData(daent.key, prev, daent.value)

        if (!deepEqual(prev, next)) {
          this._data.set(daent.key, next)
          this._dataEpoch.set(daent.key, new_epoch)
          datas_changed.add(daent.key)
        }
      }
    }

    // incorporate data changes from below
    for (let dno of this._downQueue.splice(0)) {
      if (!down_maxindex.has(dno.id)) {
        down_maxindex.set(dno.id, 0)
      }

      down_maxindex.set(dno.id, Math.max(down_maxindex.get(dno.id), dno.epoch))
      copyData(dno.data)
    }

    // incorporate injected changes
    let injected_callbacks = []
    for (let dno of this._injectQueue.splice(0)) {
      injected_callbacks.push(dno.callback)

      copyData(dno.data)
    }

    for (let sno of this._sibQueue.splice(0)) {
      if (sno.upId !== this.upId) {
        continue
      }

      if (sno.upEpoch > this._upEpoch) {
        // can't process this *yet* (all future from the same sibling will be similarly held up)
        this._sibQueue.push(sno)
        this.addTrigger(sno.upId, sno.upEpoch)
        continue
      }

      copyClocks(sno.clocks)
      copyData(sno.data)
    }

    // calculate changes to our index values
    for (let ixkey of this._myIndices.keys()) {
      let { value } = this._binding.calculateIndex(this, ixkey)
      if (!deepEqual(value, this._myIndices.get(ixkey))) {
        mapSet(this._myIndices, ixkey, value)
        myindex_changed.add(ixkey)
      }
    }

    // pass index changes down
    for (let ds of this._downstreams.values()) {
      let containsTo = down_maxindex.get(ds.id) || 0
      let index = []

      for (let ixkey of myindex_changed) {
        if (ds.want.has(ixkey)) {
          index.push({ key: ixkey, value: this._myIndices.get(ixkey) })
        }
      }

      if (containsTo > 0 || index.length > 0 || this.checkClockTriggers(ds)) {
        this._rpc.qcall(ds.id, 'replicate_down', {
          epoch: new_epoch, clocks: this.diffClocks(ds.sentClocks), containsTo, index
        })
        this.updatePartnerClocks(ds)
      }
    }

    let data_vec = []
    for (let dkey of datas_changed) {
      let dval = this._data.get(dkey)
      if (dval !== undefined) {
        data_vec.push({ key: dkey, value: dval })
      }
    }

    // pass data changes up
    if (this.upId && data_vec.length > 0) {
      this._rpc.qcall(this.upId, 'replicate_up', { epoch: new_epoch, data: data_vec })
    }

    for (let sib of this._siblings.values()) {
      if (data_vec.length > 0 || this.checkClockTriggers(sib)) {
        this._rpc.qcall(sib.id, 'replicate_sibling', {
          upId: this.upId,
          upEpoch: this._upEpoch,
          clocks: clocks_changed,
          data: data_vec
        })

        this.updatePartnerClocks(sib)
      }
    }

    // pass to callbacks
    for (let cb of injected_callbacks) {
      cb()
    }

    this._ancestorTriggers = this._ancestorTriggers.filter(({ system, clock, cb }) => {
      if (this._ancestorClocks.get(system) >= clock) {
        if (cb) cb()
      } else {
        return true
      }
    })

    for (let barrier of this._barrierQueue.splice(0)) {
      this.doBarrier(barrier.target).then(epoch => {
        this._rpc.qcall(barrier.from, 'barrier_down', { epoch, id: barrier.id })
      })
    }
  }

  epochSoon () {
    if (!this._epochTimer) {
      this._epochTimer = setTimeout(() => {
        this._epochTimer = 0
        this.newEpoch()
      }, 500)
    }
  }

  rpc_replicate_down (args) {
    this._upQueue.push(args)
    this.epochSoon()
  }

  rpc_replicate_up (args) {
    args.id = args.FROM
    this._downQueue.push(args)
    this.epochSoon()
  }

  rpc_replicate_sibling (args) {
    this._sibQueue.push(args)
    this.epochSoon()
  }

  rpc_put (args) {
    let { promise, resolver } = promiseAndResolver()
    this._injectQueue.push({
      data: args.data,
      callback: () => {
        resolver({ epoch: this._epoch })
      }
    })
    this.epochSoon()
    return promise
  }

  addTrigger (system, clock, cb) {
    if (!this._ancestorTriggers.some(trig => trig.system === system && trig.clock === clock)) {
      // this is a wholly new trigger!  notify our rsibs and ancestors that We Care about this and they should send it even if there's no salient data
      if (this.upId) {
        this._rpc.qcall(this.upId, 'notify_trigger', { system, clock })
      }
      for (let rs of this._rsiblings.values()) {
        this._rpc.qcall(rs.id, 'notify_trigger', { system, clock })
      }
    }
    if ((this._ancestorClocks.get(system) || 0) >= clock) {
      if (cb) cb()
    } else {
      this._ancestorTriggers.push({ system, clock, cb })
    }
  }

  rpc_notify_trigger (args) {
    // one of our downstreams and/or siblings is waiting patiently for version BLAH.

    for (let partner of [this._downstreams, this._siblings].map(x => x.get(args.FROM)).filter(y => y)) {
      // maybe our messages just crossed in the 'net.
      if ((partner.sentClocks.get(args.system) || 0) >= args.clock) {
        // they'll get it in due time.
        continue
      }

      if ((this._ancestorClocks.get(args.system) || 0) >= args.clock) {
        // we have it, but never sent it.  send it now.
        // we know there are no data/index changes because those force a send.
        // (as does containsTo)
        if ('want' in partner) {
          this._rpc.qcall(partner.id, 'replicate_down', {
            epoch: this._epoch, clocks: this.diffClocks(partner.sentClocks), containsTo: 0, index: []
          })
          this.updatePartnerClocks(partner)
        } else {
          this._rpc.qcall(partner.id, 'replicate_sibling', {
            upId: this.upId, upEpoch: this._upEpoch, clocks: this.diffClocks(partner.sentClocks), data: []
          })
          this.updatePartnerClocks(partner)
        }
        continue
      }

      // we don't have it.  remember they want it, and make sure *we* get it
      partner.triggerClocks.push({ system: args.system, clock: args.clock })
      this.addTrigger(args.system, args.clock)
    }
  }

  updatePartnerClocks (partner) {
    partner.sentClocks = new Map(this._ancestorClocks)
    partner.triggerClocks = partner.triggerClocks.filter(({ system, clock }) => {
      return (this._ancestorClocks.get(system) || 0) < clock
    })
  }

  checkClockTriggers (partner) {
    return partner.triggerClocks.some(({ system, clock }) => {
      return (this._ancestorClocks.get(system) || 0) >= clock
    })
  }

  rpc_acquire (args) {
    let { promise, resolver } = promiseAndResolver()
    this.addTrigger('' + args.system, args.clock, resolver)
    return promise
  }

  waitForIndices (keys) {
    let request = new Set()
    let ready = true
    for (let key of keys) {
      if (this._myIndices.get(key) !== undefined) {
        continue
      }

      let { value, need } = this._binding.calculateIndex(this, key)
      // console.log(key, value, need)
      if (value !== undefined) {
        this._myIndices.set(key, value)
        continue
      }

      for (let req of need) {
        request.add(req)
      }
      ready = false
      if (request.size === 0 || !this.upId) {
        throw new Error('calculateRequirements invalidly returned empty')
      }
    }

    if (ready) {
      return Promise.resolve(null)
    }

    // otherwise we should subscribe for *request* from our upstream and continue
    let id = this._nextSubId++
    let subscribed = new Promise((resolve, reject) => this._subCallbacks.set(id, resolve))

    this._rpc.qcall(this.upId, 'subscribe', { id, keys: Array.from(request) }) // does not need to be anchored to epoch
    return subscribed.then(() => this.waitForIndices(keys))
  }

  dumpClocks () {
    return Array.from(this._ancestorClocks).map(ent => ({ system: ent[0], clock: ent[1] }))
  }

  diffClocks (old) {
    return this.dumpClocks().filter(({ system, clock }) => clock > (old.get(system) || 0))
  }

  rpc_connect_sibling (args) {
    args.id = '' + args.id
    this._siblings.set(args.id, { id: args.id, sentClocks: new Map(this._ancestorClocks), triggerClocks: [] })

    let msg = {
      upId: this.upId,
      upEpoch: this._upEpoch,
      clocks: this.dumpClocks(),
      data: []
    }

    for (let entry of this._data) {
      msg.data.push({ key: entry[0], value: entry[1] })
    }

    this._rpc.qcall(args.id, 'connect_rsibling', {})
    this._rpc.qcall(args.id, 'replicate_sibling', msg)
  }

  rpc_connect_rsibling (args) {
    this._rsiblings.set(args.FROM, { id: args.FROM })
    for (let trig of this._ancestorTriggers) {
      // make sure it knows what we're waiting for
      this._rpc.qcall(args.FROM, 'notify_trigger', { system: trig.system, clock: trig.clock })
    }
  }

  initok_become_root () {}
  rpc_become_root (args) {
    if (this._upId !== undefined) {
      throw new Error('too late to become a root')
    }

    this._upId = null
    this._openArc = '' + this._rpc.id
    ;(this._upIdResolver)()
  }

  initok_connect_up () {}
  rpc_connect_up (args) {
    if (this._upId !== undefined) {
      throw new Error('too late to become a cache')
    }

    this._upId = '' + args.id
    this._upEpoch = 0
    this._rpc.qcall(args.id, 'connect_down', {})
  }

  initok_assign_arc () {}
  rpc_assign_arc (args) {
    this._openArc = args.arc
    ;(this._upIdResolver)()
    this.waitForIndices([ 'p' + this._openArc ])
  }

  allocateArc () {
    return `${this._openArc}.${this._nextArc++}`
  }

  rpc_connect_down (args) {
    this._downstreams.set(args.FROM, { id: args.FROM, want: new Set(), sentClocks: new Map(this._ancestorClocks), triggerClocks: [] })
    this._rpc.qcall(args.FROM, 'assign_arc', { arc: this.allocateArc() })
    this._rpc.qcall(args.FROM, 'replicate_down', { epoch: this._epoch, clocks: this.dumpClocks(), containsTo: 0, index: [] })
  }

  rpc_subscribe_down (args) {
    if (this._upQueue.length > 0) {
      this.newEpoch() // may require fiddling for distrib
    }

    for (let ixent of args.index) {
      let prev = this._upIndices.get(ixent.key)
      if (!deepEqual(prev, ixent.value)) {
        mapSet(this._upIndices, ixent.key, ixent.value)
      }
    }

    let cb = this._subCallbacks.get(args.id)
    this._subCallbacks.delete(args.id)
    cb()
  }

  rpc_barrier_down (args) {
    let cb = this._barrierCallbacks.get(args.id)
    this._barrierCallbacks.delete(args.id)
    cb(args.epoch)
  }

  rpc_barrier_up (args) {
    // we can short-circuit this in many cases
    this._barrierQueue.push({ target: args.target, id: args.id, from: args.FROM })
    this.epochSoon()
  }

  doBarrier (target) {
    if (!this.upId) {
      return Promise.resolve(0)
    }
    if (target === this._rpc.id) {
      return Promise.resolve(this._epoch)
    }
    let id = this._barrierNextId++
    let { promise, resolver } = promiseAndResolver()
    this._barrierCallbacks.set(id, resolver)
    this._rpc.qcall(this.upId, 'barrier_up', { target, id })
    return promise
  }

  rpc_barrier (args) {
    return this.doBarrier('' + args.target)
  }

  rpc_get (args) {
    return this.waitForIndices([args.key]).then(() => {
      return { value: this._myIndices.get(args.key) }
    })
  }

  rpc_subscribe (args) {
    this.waitForIndices(args.keys).then(() => {
      let ds = this._downstreams.get(args.FROM)
      let index = []
      for (let key of args.keys) {
        if (ds.want.has(key)) continue
        ds.want.add(key)
        index.push({ key, value: this._myIndices.get(key) })
      }
      this._rpc.qcall(args.FROM, 'subscribe_down', { index, id: args.id })
    })
  }

  rpc_eav_put (args) {
    if (!args.id) args.id = this.allocateArc()
    let { promise, resolver } = promiseAndResolver()
    this._injectQueue.push({
      data: Object.keys(args.data).map(kk => {
        return {
          key: `${args.table},${args.id},${kk}`,
          value: { d: args.data[kk], v: Date.now() }
        }
      }),
      callback: resolver
    })
    this.epochSoon()
    return promise.then(() => ({ id: args.id, epoch: this._epoch }))
  }

  rpc_eav_get (args) {
    let ikey = `e${args.table},${args.id}`
    return this.waitForIndices([ikey]).then(() => {
      let res = {}
      for (let dd of this._myIndices.get(ikey)) {
        let kparts = dd[0].split(',')
        res[kparts[2]] = dd[1].d
      }
      return res
    })
  }

  rpc_eav_search (args) {
    let ikey = args.col ? `r${args.table},${args.col},${args.value}` : `t${args.table}`
    return this.waitForIndices([ikey]).then(() => {
      return Array.from(new Set(this._myIndices.get(ikey).map(dd => dd[0].split(',')[1])))
    })
  }
}

process.env.PORT.split(',').forEach(port => {
  let rpc = new RPC(port)
  let state = new State(rpc)
  rpc.server((cmd, args) => {
    let fn = state[`rpc_${cmd}`]
    if (fn) {
      let pp = []
      if (!state[`initok_${cmd}`]) pp.push(state._upIdPromise)
      if ('CLOCK' in args) pp.push(waitUntil(args.CLOCK))
      // should diagnose an error and drop packets if >2s future
      return Promise.all(pp).then(() => fn.call(state, args))
    } else {
      throw new Error('invalid command')
    }
  })
})
