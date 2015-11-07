'use strict'
const http = require('http')
const got = require('got')

function netdelay () {
  return 500 * (Math.random() + 0.5)
}

let next_id = 0
function rpcServer (port, handler) {
  http.createServer((req, res) => {
    let parts = []
    let id = next_id++
    req.on('data', part => parts.push(part))
    req.on('end', () => {
      let name = require('url').parse(req.url).pathname.replace(/\//g, '')
      new Promise((resolve, reject) => {
        let text = Buffer.concat(parts).toString('utf8')
        let json = JSON.parse(text)
        setTimeout(() => {
          console.log(Date.now(), 'IN<-', id, name, text)
          resolve(json)
        }, netdelay())
      }).then(json => handler(name, json)).then(
        result => [200, new Buffer(JSON.stringify(result), 'utf8')],
        error => [500, new Buffer(error.toString(), 'utf8')]
      ).then(code_buf => {
        console.log(Date.now(), 'IN->', id, code_buf[1].toString('utf8'))
        setTimeout(() => {
          res.writeHead(code_buf[0], {
            'Content-Length': code_buf[1].length,
            'Content-Type': 'application/json'
          })
          res.end(code_buf[1])
        }, netdelay())
      })
    })
  }).listen(port)
}

function rpcCall (port, name, args) {
  let id = next_id++
  console.log(Date.now(), 'OU->', id, name, JSON.stringify(args))
  let prom = got.post(`http://localhost:${port}/${name}`,
    { body: JSON.stringify(args) }).then(response => response.body)

  prom.then(
    result => console.log(Date.now(), 'OU<-', id, result),
    error => console.log(Date.now(), 'OU<-', id, error.toString())
  )

  return prom
}

// for the purpose of this example, our payload is a set of strings keyed by a string
// we track our present state, clocks?, peerings?
class State {
  constructor (id) {
    this._id = id
    this._data = {}
    this._epoch = 0 // epoch 0 is ALWAYS empty
    this._clocks = {} // Not part of the cut, advisory only

    this._pending = []
    this._commitTimer = 0
    this._commitPromise = null

    this._peers = {}
  }

  get id () {
    return this._id
  }

  _commit () {
    let change = []
    let data_changed = false
    this._pending.forEach(pend => {
      if ('epoch' in pend) {
        if ((this._clocks[pend.system] || 0) >= pend.epoch) return
        change.push(pend)
        this._clocks[pend.system] = pend.epoch
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
    }
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
    let port = args.port
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
    let port = args.port
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

    return rpcCall(port, 'hello', {
      port: this.id, passClocks: !!(args.clock & 2)
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

    rpcCall(port, 'update', {
      from_epoch: from_epoch, to_epoch: this._epoch, data: queue, port: this.id
    }).then(() => {
      peer.sending = false
      this._checkSendUpdate(port)
    })
  }

  rpc_update (args) {
    let peer = this._peers[args.port]
    args.data.forEach(item => {
      if ('epoch' in item && !peer.passClocks) return
      this._pending.push(item)
    })
    return this._commitSoon()
  }

  rpc_get (args) {
    return this._data[args.key] || {}
  }

  rpc_clocks (args) {
    return this._clocks
  }

  rpc_put (args) {
    args.data.forEach(item => {
      this._pending.push({ key: String(item.key), value: String(item.value) })
    })
    return this._commitSoon()
  }
}

{
  let state = new State(process.env.PORT)
  rpcServer(state.id, (cmd, args) => {
    let fn = state[`rpc_${cmd}`]
    if (fn) {
      return fn.call(state, args)
    } else {
      throw new Error('invalid command')
    }
  })
}
