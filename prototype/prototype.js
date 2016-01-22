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
    let clearNext
    let newReady = new Promise((resolve, reject) => clearNext = resolve)
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

class Binding {
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
    // { containsTo: 5, index: [{"key": 5}, {"key": 6, "value": 2}] }
    this._upQueue = []
    // Incoming updates from downstream
    // { id: 1, epoch: 17, data: [{"key": "A", "value": 1}] }
    this._downQueue = []
    this._injectQueue = []

    // Map of index keys to opaque values
    this._myIndices = new Map()

    // map of IDs to { id: 15, want: Set(...) }
    this._downstreams = new Map()
  }

  newEpoch () {
    let datas_changed = new Set()
    let upindex_changed = new Set()
    let myindex_changed = new Set()
    let down_maxindex = new Map()

    let new_epoch = ++this._epoch

    // incorporate index changes from above
    for (let upo of this._upQueue.splice()) {
      for (let ent of this._dataEpoch) {
        if (ent[1] <= upo.containsTo) {
          this._dataEpoch.delete(ent[0])
          this._data.delete(ent[0])
          datas_changed.add(ent[0])
        }
      }

      for (let ixent of upo.index) {
        let prev = this._upIndices.get(ixent.key)
        if (!deepEqual(prev, ixent.value)) {
          upindex_changed.add(ixent.key)
          mapSet(this._upIndices, ixent.key, ixent.value)
        }
      }
    }

    // incorporate data changes from below
    for (let dno of this._downQueue.splice()) {
      if (!down_maxindex.has(dno.id)) {
        down_maxindex.set(dno.id, 0)
      }

      down_maxindex.set(dno.id, Math.max(down_maxindex.get(dno.id), dno.epoch))

      for (let daent of dno.data) {
        let prev = this._data.get(daent.key)
        let next = this._binding.lubData(prev, daent.value)

        if (!deepEqual(prev, next)) {
          this._data.set(daent.key, next)
          this._dataEpoch.set(daent.key, new_epoch)
          datas_changed.add(daent.key)
        }
      }
    }

    // incorporate injected changes
    let injected_callbacks = []
    for (let dno of this._injectQueue.splice()) {
      injected_callbacks.push(dno.callback)

      for (let daent of dno.data) {
        let prev = this._data.get(daent.key)
        let next = this._binding.lubData(prev, daent.value)

        if (!deepEqual(prev, next)) {
          this._data.set(daent.key, next)
          this._dataEpoch.set(daent.key, new_epoch)
          datas_changed.add(daent.key)
        }
      }
    }

    // HERE sibling messages

    // calculate changes to our index values
    for (let ixkey of this._myIndices.keys()) {
      let recalc = this._binding.calculateIndex(this, ixkey)
      if (JSON.stringify(recalc) !== JSON.stringify(this._myIndices.get(ixkey))) {
        mapSet(this._myIndices, ixkey, recalc)
        myindex_changed.add(ixkey)
      }
    }

    // pass index changes down
    for (let ds of this._downstreams.values()) {
      let msg = {
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

    // pass to callbacks
    for (let cb of injected_callbacks) {
      cb()
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
    this._upQueue.push(args.msg)
    this.epochSoon()
  }

  rpc_replicate_up (args) {
    this._downQueue.push(args.msg)
    this.epochSoon()
  }

  rpc_put (args) {
    let resolver
    let promise = new Promise((resolve, reject) => resolver = resolve)
    this._injectQueue.push({
      data: args.data,
      callback: () => {
        resolver({ epoch: this._epoch })
      }
    })
    this.epochSoon()
    return promise
  }

  rpc_get (args) {
    let val = this._myIndices.get(args.key)
    if (val !== undefined) {
      return { value: val }
    }

    val = this._binding.calculateIndex(this, args.key)
    if (val !== undefined) {
      return { value: val }
    }

    // let reqm = this._binding.calculateRequirements(this, args.key)
    throw new Error('asdf')
    // return this._rpc.call(this._upId, 'subscribe')
  }

  rpc_subscribe (args) {
    throw new Error('asdf')
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
