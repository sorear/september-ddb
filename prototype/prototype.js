'use strict'
const http = require('http')
const got = require('got')

class RPC {
  constructor (id) {
    this._id = id
    this._next_id = 1
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
}

class DMap extends Map {
  constructor (deffn) {
    super()
    this._default = deffn
  }

  open (key) {
    if (this.has(key)) {
      return this.get(key)
    }
    let nval = (this._default)()
    this.set(key, nval)
    return nval
  }

  fetch (key) {
    return this.has(key) ? this.get(key) : (this._default)()
  }
}

// for the purpose of this example, our payload is a set of strings keyed by a string
// we track our present state, clocks?, peerings?
class State {
  constructor (rpc) {
    // relevant to the cut
    this._id = rpc.id
    this._epoch = 0 // epoch 0 is ALWAYS empty
    this._version = 0
    this._clocks = {}
    // modifying _clocks changes _epoch but not _version

    this._urdata = new DMap(() => new Map())
    this._baseIndices = new Map()
    this._ourIndices = new Map()

    // not relevant to the cut
    this._rpc = rpc
    this._pending = []
    this._commitTimer = 0

    // sent can grow without bound currently.  FUTURE: fix that (it doesn't replicate so it's not so bad)
    this._callbacksSent = {}
    this._callbacksReceived = {}

    this._peers = {}
    this._basePeer = null
  }

  get id () {
    return this._id
  }

  // TODO: since we rely on back-replication to invoke the callbacks, they fire one round trip too late when replicating down
  _commit (pending) {
    let clock_updates = []
    let data_updates = []
    let fire_callback = {}
    let new_version = false

    this._epoch++

    for (let update of pending) {
      let peer = update.peer // may be null for non-update events
      if (peer && peer.failedReplication) {
        update.reject(update.peer.failedReplication)
        continue
      }

      if (update.base) {
        if (update.base.system === this.id) {
          // someone updating based on us.  OK
        } else if (update.base.system === peer.port && peer === this._basePeer) {
          // someone thinks we're their downstream
          if (update.base.epoch > peer.baseEpoch) {
            peer.baseEpoch = update.base.epoch
            new_version = true
          }

          if (update.filterChanges) {
            let filter = peer.receivingFilter
            if (!filter) {
              filter = peer.receivingFilter = {
                points: [],
                prefixes: []
              }
            }

            if (update.filterChanges.addPoints) {
              filter.points = filter.points.concat(update.filterChanges.addPoints)
            }

            if (update.filterChanges.addPrefixes) {
              filter.prefixes = filter.points.concat(update.filterChanges.addPrefixes)
            }
          }
        } else {
          update.reject(update.peer.failedReplication = 'unexpected base')
          continue
        }
      }

      for (let clock_up of update.clocks_included || []) {
        if ((this._clocks[clock_up.system] || 0) >= clock_up.epoch) continue
        clock_updates.push(clock_up)
        this._clocks[clock_up.system] = clock_up.epoch
      }

      for (let callback of update.callbacks || []) {
        if (this._callbacksSent[callback.target] &&
          this._callbacksSent[callback.target] >= callback.epoch) continue
        if (fire_callback[callback.target] &&
          fire_callback[callback.target].epoch >= callback.epoch) continue
        fire_callback[callback.target] = callback
      }

      // only for not-the-base
      for (let item of update.urdata || []) {
        let schema_bucket = this._urdata.open(item.schema)
        let key_bucket = schema_bucket.open(item.key)
        if (TypedData.updateUr(key_bucket, item.delta)) {
          key_bucket.stamp = this._epoch + 1
          data_updates.push(item)
          new_version = true
        }
      }

      // only for the base
      for (let item of update.cachedata || []) {
        let index_bucket = this._cachedata.open(item.index)
        let key_bucket = index_bucket.open(item.key)

      }

      update.resolve(null) // can't actually fire until the dust is settled
    }

    let forward_callbacks = []

    if (new_version) {
      // merely learning new clock values does not create a new version, although it is replicated
      this._version = this._epoch
      this._clocks[this.id] = this._epoch
      clock_updates.push({ system: this.id, epoch: this._epoch })
      forward_callbacks.push({ target: this.id, path: [], epoch: this._epoch })
    }

    for (let sys in fire_callback) {
      let pending_callback = fire_callback[sys]
      this._callbacksSent[pending_callback.target] = pending_callback.epoch
      this._rpc.call(pending_callback.path[0], 'callback', {
        epoch: pending_callback.epoch, into: this.id, into_epoch: this._version,
        path: pending_callback.path.slice(1)
      })
      forward_callbacks.push(pending_callback)
    }

    for (let port in this._peers) {
      let peer = this._peers[port]
      if (this._basePeer && peer !== this._basePeer) {
        throw "don't know how to replicate down from a cache"
      }

      let message = {
        base: peer.sendingFilter ? { system: this.id, epoch: this._epoch } : null,
        data: data_updates.filter(upd => this._keyMatchesFilter(upd.key, peer.sendingFilter)),
        clocks_included: peer.isClockSource ? [] : clock_updates,
        // the callbacks should retrace the replication path
        callbacks: peer.isClockSource ? forward_callbacks.map(qe => {
          return { target: qe.target, path: [this.id].concat(qe.path), epoch: qe.epoch }
        }) : []
      }

      if (message.base || message.data.length || message.clocks_included.length || message.callbacks.length) {
        peer.sendQueue.push(message)
        this._checkSendUpdate(peer)
      }
    }
  }

  // implements nomination/arbiter process
  // will take ownership of object and inject resolve() and reject() methods
  _nominate (obj) {
    let promise = new Promise((resolve, reject) => {
      obj.resolve = resolve
      obj.reject = reject
    })
    this._pending.push(obj)
    if (this._pending.length === 1) {
      this._commitTimer = setTimeout(() => {
        this._commitTimer = 0
        let pending = this._pending
        this._pending = []
        this._commit(pending)
      }, 200 * (Math.random() + 1))
    }
    return promise
  }

  rpc_hello (args) {
    let port = args.FROM
    if (this._peers[port]) {
      throw new Error('Already peered')
    }

    if (this._basePeer) {
      throw "don't know how to replicate from a cache"
    }

    let peer = this._peers[port] = {
      cork: false,
      port: port,
      isClockSource: args.isClockSource,
      sending: false,
      sendingFilter: args.filter,
      receivingFilter: null,
      failedReplication: null,
      sendQueue: [this._dumpAll(args.filter)]
    }

    this._checkSendUpdate(peer)
    return null
  }

  rpc_connect (args) {
    let port = args.peer
    if (this._peers[port]) {
      throw new Error('Already peered')
    }

    if (args.filter && args.isClockSink) {
      throw new Error('A partial replica cannot be a clock source')
    }

    if (this._basePeer) {
      throw "don't know how to replicate from a cache"
    }

    let peer = this._peers[port] = {
      cork: true, // don't send updates until they have acked the Hello
      port: port,
      isClockSource: !!args.isClockSource,
      sending: false,
      sendingFilter: null,
      receivingFilter: null,
      failedReplication: null,
      sendQueue: [this._dumpAll(null)]
    }

    if (args.filter) {
      this._basePeer = peer
    }

    return this._rpc.call(port, 'hello', {
      isClockSource: !!args.isClockSink,
      filter: args.filter
    }).then(() => {
      peer.cork = false
      this._checkSendUpdate(peer)
      return null
    }, e => {
      if (this._basePeer === peer) {
        this._basePeer = null
      }
      if (this._peers[port] === peer) {
        delete this._peers[port]
      }
      throw e
    })
  }

  _keyMatchesFilter (key, filter) {
    return !filter || filter.points.indexOf(key) >= 0 || filter.prefixes.some(prefix => key.startsWith(prefix))
  }

  _dumpAll (filter) {
    let data = []
    let clocks = []
    for (let key of this._data.keys()) {
      if (!this._keyMatchesFilter(key, filter)) {
        continue
      }
      for (let value of this._data.get(key)) {
        data.push({ key: key, value: value })
      }
    }
    for (let sys in this._clocks) {
      clocks.push({ system: sys, epoch: this._clocks[sys] })
    }
    return {
      base: filter ? { system: this.id, epoch: this._epoch } : null,
      filterChanges: filter ? {
        addPoints: filter.points,
        addPrefixes: filter.prefixes
      } : null,
      data: data,
      clock_updates: clocks
    }
  }

  _checkSendUpdate (peer) {
    if (peer.cork || peer.sending || !peer.sendQueue.length) return
    let updates = peer.sendQueue

    peer.sending = true
    peer.sendQueue = []

    this._rpc.call(peer.port, 'update', {
      updates
    }).then(() => {
      peer.sending = false
      this._checkSendUpdate(peer)
    })
  }

  rpc_update (args) {
    let peer = this._peers[args.FROM]
    for (let epoch of args.updates) {
      epoch.peer = peer
      this._nominate(epoch)
    }
    return null // just queueing
  }

  rpc_get (args) {
    if (this._basePeer && !this._keyMatchesFilter(args.key, this._basePeer.receivingFilter)) {
      throw "don't know how to get from a cache"
    }

    return Array.from(this._data.get(args.key) || new Set())
  }

  rpc_callback (args) {
    if (args.path.length > 0) {
      return this._rpc.call(args.path[0], 'callback', {
        epoch: args.epoch, into_epoch: args.into_epoch, into: args.into,
        path: args.path.slice(1)
      })
    } else {
      if (!this._callbacksReceived[args.into] ||
        args.epoch >= this._callbacksReceived[args.into].epoch) {
        this._callbacksReceived[args.into] = args
      }
      return null
    }
  }

  rpc_clocks (args) {
    let out = {}
    for (let sys in this._clocks) {
      if (!out[sys]) out[sys] = {}
      out[sys].i_have = this._clocks[sys]
    }
    for (let sys in this._callbacksReceived) {
      if (!out[sys]) out[sys] = {}
      out[sys].they_have = this._callbacksReceived[sys].epoch
      out[sys].they_have_in = this._callbacksReceived[sys].into_epoch
    }
    return out
  }

  rpc_put (args) {
    let data = []
    args.data.forEach(item => {
      data.push({ key: String(item.key), value: String(item.value) })
    })
    return this._nominate({ type: 'put', data }) // here we do want to wait, RYW
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
