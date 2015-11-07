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
            console.log(Date.now(), this.id, 'IN<-', json.FROM, id, name, json)
            resolve(json)
          }, this.netdelay())
        }).then(json => handler(name, json)).then(
          result => [200, new Buffer(JSON.stringify(outdata = result), 'utf8')],
          error => [500, new Buffer((outdata = error).toString(), 'utf8')]
        ).then(code_buf => {
          console.log(Date.now(), this.id, 'IN->', json.FROM, id, outdata)
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
    console.log(Date.now(), this.id, 'OU->', dest, id, name, args)
    let prom = got.post(`http://localhost:${dest}/${name}`,
      { body: JSON.stringify(args), json: true }).then(response => response.body)

    prom.then(
      result => console.log(Date.now(), this.id, 'OU<-', dest, id, result),
      error => console.log(Date.now(), this.id, 'OU<-', dest, id, error.message)
    )

    return prom
  }
}

// for the purpose of this example, our payload is a set of strings keyed by a string
// we track our present state, clocks?, peerings?
class State {
  constructor (rpc) {
    this._rpc = rpc
    this._id = rpc.id
    this._data = {}
    this._epoch = 0 // epoch 0 is ALWAYS empty
    this._clocks = {} // Not part of the cut, advisory only

    this._pending = []
    this._commitTimer = 0
    this._commitPromise = null

    // sent can grow without bound currently.  FUTURE: fix that (it doesn't replicate so it's not so bad)
    this._callbacksSent = {}
    this._callbacksReceived = {}

    this._peers = {}
  }

  get id () {
    return this._id
  }

  // TODO: since we rely on back-replication to invoke the callbacks, they fire one round trip too late when replicating down
  _commit () {
    let change = []
    let data_changed = false
    let fire_callback = {}
    this._pending.forEach(pend => {
      if ('system' in pend) {
        if ((this._clocks[pend.system] || 0) >= pend.epoch) return
        change.push(pend)
        this._clocks[pend.system] = pend.epoch
      } else if ('callback' in pend) {
        if (this._callbacksSent[pend.callback] &&
          this._callbacksSent[pend.callback] >= pend.epoch) return
        if (fire_callback[pend.callback] &&
          fire_callback[pend.callback].epoch >= pend.epoch) return
        fire_callback[pend.callback] = pend
      } else {
        this._data[pend.key] = this._data[pend.key] || {}
        if (this._data[pend.key][pend.value]) return
        change.push(pend)
        data_changed = true
        this._data[pend.key][pend.value] = true
      }
    })
    if (data_changed) {
      // merely learning new clock values does not create a new epoch, although it is replicated
      this._epoch++
      this._clocks[this.id] = this._epoch
      change.push({ system: this.id, epoch: this._epoch })
      change.push({ callback: this.id, path: [], epoch: this._epoch })
    }
    Object.keys(fire_callback).forEach(sys => {
      let pend = fire_callback[sys]
      this._callbacksSent[pend.callback] = pend.epoch
      this._rpc.call(pend.path[0], 'callback', {
        epoch: pend.epoch, into: this.id, into_epoch: this._epoch,
        path: pend.path.slice(1)
      })
      change.push(pend)
    })
    if (change.length) {
      Object.keys(this._peers).forEach(port => {
        if (this._peers[port].valid) {
          change.forEach(pend => this._peers[port].sendQueue.push(pend))
          this._checkSendUpdate(port)
        }
      })
    }
    this._pending = []
    this._commitTimer = 0
    return null
  }

  _commitSoon () {
    if (!this._pending.length) return Promise.resolve(null)
    if (!this._commitTimer) {
      this._commitPromise = new Promise((resolve, reject) => {
        this._commitTimer = setTimeout(() => resolve(this._commit()),
          200 * (Math.random() + 1))
      })
    }
    return this._commitPromise
  }

  rpc_hello (args) {
    let port = args.FROM
    if (this._peers[port]) {
      throw new Error('Already peered')
    }

    this._peers[port] = {
      valid: true,
      port: port,
      sending: false,
      sentEpoch: 0,
      passClocks: args.passClocks,
      sendQueue: this._dumpAll()
    }

    this._checkSendUpdate(port)
    return null
  }

  rpc_connect (args) {
    let port = args.peer
    if (this._peers[port]) {
      throw new Error('Already peered')
    }

    this._peers[port] = {
      valid: false,
      port: port,
      passClocks: !!(args.clock & 1),
      sending: false,
      sentEpoch: 0,
      sendQueue: this._dumpAll()
    }

    return this._rpc.call(port, 'hello', {
      passClocks: !!(args.clock & 2)
    }).then(() => {
      this._peers[port].valid = true
      this._checkSendUpdate(port)
      return null
    }, e => {
      delete this._peers[port]
      throw e
    })
  }

  _dumpAll () {
    let outp = []
    Object.keys(this._data).forEach(key => {
      Object.keys(this._data[key]).forEach(value => {
        outp.push({ key: key, value: value })
      })
    })
    Object.keys(this._clocks).forEach(sys => {
      outp.push({ system: sys, epoch: this._clocks[sys] })
    })
    return outp
  }

  _checkSendUpdate (port) {
    let peer = this._peers[port]
    if (!peer || !peer.valid || peer.sending || !peer.sendQueue.length) return
    let queue = peer.sendQueue
    let from_epoch = peer.sentEpoch

    peer.sending = true
    peer.sendQueue = []
    peer.sentEpoch = this._epoch

    if (!peer.passClocks) {
      queue = queue.filter(qe => !('callback' in qe))
    }
    queue = queue.map(qe => {
      if ('callback' in qe) {
        // the callbacks should retrace the replication path
        qe = { callback: qe.callback, path: [this.id].concat(qe.path), epoch: qe.epoch }
      }
      return qe
    })

    this._rpc.call(port, 'update', {
      from_epoch: from_epoch, to_epoch: this._epoch, data: queue
    }).then(() => {
      peer.sending = false
      this._checkSendUpdate(port)
    })
  }

  rpc_update (args) {
    let peer = this._peers[args.FROM]
    args.data.forEach(item => {
      if ('system' in item && !peer.passClocks) return
      this._pending.push(item)
    })
    return this._commitSoon()
  }

  rpc_get (args) {
    return this._data[args.key] || {}
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
    Object.keys(this._clocks).forEach(sys => {
      if (!out[sys]) out[sys] = {}
      out[sys].i_have = this._clocks[sys]
    })
    Object.keys(this._callbacksReceived).forEach(sys => {
      if (!out[sys]) out[sys] = {}
      out[sys].they_have = this._callbacksReceived[sys].epoch
      out[sys].they_have_in = this._callbacksReceived[sys].into_epoch
    })
    return out
  }

  rpc_put (args) {
    args.data.forEach(item => {
      this._pending.push({ key: String(item.key), value: String(item.value) })
    })
    return this._commitSoon()
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
