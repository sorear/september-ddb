'use strict'
const http = require('http')
const got = require('got')

class RPC {
  constructor (id) {
    this._id = id
    this._next_id = 1
    this._readyp = new Map()
  }

  get id () {
    return this._id
  }

  netdelay () {
    return 500 * (Math.random() + 0.5)
  }

  server (handler) {
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
            console.log('%d %d IN<- %s %s %s %s', Date.now(), this.id, json.FROM, id, name, JSON.stringify(json, null, 2))
            resolve(json)
          }, this.netdelay())
        }).then(json => handler(name, json)).then(
          result => [200, new Buffer(JSON.stringify(outdata = result, null, 2) + '\n', 'utf8')],
          error => [500, new Buffer(JSON.stringify(outdata = (error instanceof Error ? error.toString() : error), null, 2) + '\n', 'utf8')]
        ).then(code_buf => {
          console.log('%d %d IN-> %s %s %s', Date.now(), this.id, json.FROM, id, JSON.stringify(outdata, null, 2))
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
    let id = '#' + this._next_id++
    args.FROM = this.id
    console.log('%s %s OU-> %s %s %s %s', Date.now(), this.id, dest, id, name, JSON.stringify(args, null, 2))
    let prom = got.post(`http://localhost:${dest}/${name}`,
      { body: JSON.stringify(args, null, 2), json: true }).then(response => response.body, err => { throw err.response.body })

    prom.then(
      result => console.log('%s %s OU<- %s %s %s', Date.now(), this.id, dest, id, JSON.stringify(result, null, 2)),
      error => console.log('%s %s OU<- %s %s %s', Date.now(), this.id, dest, id, error.message)
    )

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

class Binding {
  // our data for this demo is the simplest possible LWW key-value store
  lubData (dkey, value1, value2) {
    if (value1 === undefined) return value2
    if (value2 === undefined) return value1
    return value1.v > value2.v ? value1 : value2
  }

  // index kfoo => data for key foo
  // index dbar => all data with value=bar
  // index a => EVERYTHING
  calculateIndex (state, ikey) {
    let superval = state._upIndices.get(ikey)
    let need = new Set()
    let value = null

    if (superval === undefined) {
      // currently used in all branches.  not fundamental
      need.add(ikey)
    }

    if (ikey[0] === 'k') {
      let dkey = ikey.substring(1)
      value = this.lubData(dkey, state._data.get(dkey), superval) || { d: null, v: 0 }
    } else if (ikey[0] === 'd' || ikey === 'a') {
      let match = ikey === 'a' ? null : ikey.substring(1)
      superval = new Map(superval || [])
      // a change in the current _data could
      // * add to the mapping
      for (let ent of state._data) {
        if (superval.has(ent[0])) {
          // a matching key could delete from the index.
          // since it's in the index, we know this is the most recent upstream version.
          let upd = this.lubData(ent[0], ent[1], superval.get(ent[0]))
          // upd is now the true up to date value; drop if it's no longer a match, or update
          mapSet(superval, ent[0], (match === null || upd.d === match) ? upd : undefined)
        } else {
          // a change to something not in the map could require adding it.
          // only germane if the value matches.
          if (match === null || ent[1].d === match) {
            // might still be stale
            let updata = state._upIndices.get(ent[0])
            if (state._upId && !updata) {
              // we don't know if this is stale
              need.add(ent[0])
            }

            let upd = this.lubData(ent[0], ent[1], updata)
            if (match === null || upd.d === match) {
              // genuine insert
              superval.set(ent[0], upd)
            }
          }
        }
      }
      value = Array.from(superval)
    }

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
    this._upId = null
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

    // clocks of ancestor and sibling systems which are included in our cut
    this._ancestorClocks = new Map()
    // [{ system, clock, target }]
    this._ancestorTriggers = []
  }

  newEpoch () {
    let datas_changed = new Set()
    let upindex_changed = new Set()
    let myindex_changed = new Set()
    let down_maxindex = new Map()
    let clocks_changed = []

    let new_epoch = ++this._epoch

    function copyClocks (list) {
      for (let entry of list) {
        if (entry.clock > (this._ancestorClocks.get(entry.system) || 0)) {
          this._ancestorClocks.set(entry.system, entry.clock)
          clocks_changed.push(entry)
        }
      }
    }

    copyClocks([ { system: this._rpc.id, clock: new_epoch } ])

    // incorporate index changes from above
    for (let upo of this._upQueue.splice()) {
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

    function copyData (data) {
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
    for (let dno of this._downQueue.splice()) {
      if (!down_maxindex.has(dno.id)) {
        down_maxindex.set(dno.id, 0)
      }

      down_maxindex.set(dno.id, Math.max(down_maxindex.get(dno.id), dno.epoch))
      copyData(dno.data)
    }

    // incorporate injected changes
    let injected_callbacks = []
    for (let dno of this._injectQueue.splice()) {
      injected_callbacks.push(dno.callback)

      copyData(dno.data)
    }

    for (let sno of this._sibQueue.splice()) {
      if (sno.upId !== this._upId) {
        continue
      }

      if (sno.upEpoch > this._upEpoch) {
        // can't process this *yet* (all future from the same sibling will be similarly held up)
        this._sibQueue.push(sno)
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
      let msg = {
        epoch: new_epoch,
        clocks: clocks_changed,
        containsTo: down_maxindex.get(ds.id) || 0,
        index: []
      }

      for (let ixkey of myindex_changed) {
        if (ds.want.has(ixkey)) {
          msg.index.push({ key: ixkey, value: this._myIndices.get(ixkey) })
        }
      }

      this._rpc.qcall(ds.id, 'replicate_down', { msg })
    }

    // pass data changes up
    if (this._upId) {
      let msg = {
        id: this._rpc.id,
        epoch: new_epoch,
        data: []
      }

      for (let dkey of datas_changed) {
        let dval = this._data.get(dkey)
        if (dval !== undefined) {
          msg.data.push({ key: dkey, value: dval })
        }
      }

      this._rpc.qcall(this._upId, 'replicate_up', { msg })
    }

    for (let sib of this._siblings.values()) {
      let msg = {
        upId: this._upId,
        upEpoch: this._upEpoch,
        clocks: clocks_changed,
        data: []
      }

      for (let dkey of datas_changed) {
        let dval = this._data.get(dkey)
        if (dval !== undefined) {
          msg.data.push({ key: dkey, value: dval })
        }
      }

      this._rpc.qcall(sib.id, 'replicate_sibling', { msg })
    }

    // pass to callbacks
    for (let cb of injected_callbacks) {
      cb()
    }

    this._ancestorTriggers = this._ancestorTriggers.filter(({ system, clock, cb }) => {
      if (this._ancestorClocks.get(system) >= clock) {
        cb()
      } else {
        return true
      }
    })
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
    this._upQueue.push(args.msg)
    this.epochSoon()
  }

  rpc_replicate_up (args) {
    this._downQueue.push(args.msg)
    this.epochSoon()
  }

  rpc_replicate_sibling (args) {
    this._sibQueue.push(args.msg)
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

  rpc_acquire (args) {
    let { promise, resolver } = promiseAndResolver()
    this._ancestorTriggers.push({
      system: args.system,
      clock: args.clock,
      cb: resolver
    })
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
      if (value !== undefined) {
        this._myIndices.set(key, value)
        continue
      }

      for (let req of need) {
        request.add(req)
      }
      ready = false
      if (request.size === 0 || !this._upId) {
        throw new Error('calculateRequirements invalidly returned empty')
      }
    }

    if (ready) {
      return Promise.resolve(null)
    }

    // otherwise we should subscribe for *request* from our upstream and continue
    let id = this._nextSubId++
    let subscribed = new Promise((resolve, reject) => this._subCallbacks.set(id, resolve))

    this._rpc.qcall(this._upId, 'subscribe', { id, keys: Array.from(request) }) // does not need to be anchored to epoch
    return subscribed.then(() => this.waitForIndices(keys))
  }

  dumpClocks () {
    return Array.from(this._ancestorClocks).map(ent => ({ system: ent[0], clock: ent[1] }))
  }

  rpc_connect_sibling (args) {
    this._siblings.set(args.id, { id: args.id })

    let msg = {
      upId: this._upId,
      upEpoch: this._upEpoch,
      clocks: this.dumpClocks(),
      data: []
    }

    for (let entry of this._data) {
      msg.data.push({ key: entry[0], value: entry[1] })
    }

    this._rpc.qcall(args.id, 'connect_rsibling', {})
    this._rpc.qcall(args.id, 'replicate_sibling', { msg })
  }

  rpc_connect_rsibling (args) {
    this._rsiblings.set(args.FROM, { id: args.FROM })
  }

  rpc_connect_up (args) {
    if (this._upId || this._downstreams.size > 0 || this._data.size > 0) {
      throw new Error('too late to become a cache')
    }

    this._upId = args.id
    this._upEpoch = 0
    this._rpc.qcall(args.id, 'connect_down', {})
  }

  rpc_connect_down (args) {
    this._downstreams.set(args.FROM, { id: args.FROM, want: new Set() })
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
    return null
  }

  rpc_get (args) {
    this.waitForIndices([args.key]).then(() => {
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
    return null
  }
}

process.env.PORT.split(',').forEach(port => {
  let rpc = new RPC(port)
  let state = new State(rpc)
  rpc.server((cmd, args) => {
    let fn = state[`rpc_${cmd}`]
    if (fn) {
      return fn.call(state, args)
    } else {
      throw new Error('invalid command')
    }
  })
})
