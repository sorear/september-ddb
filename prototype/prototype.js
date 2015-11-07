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

    this._pending = []
    this._commitTimer = 0
    this._commitPromise = null
  }

  get id () {
    return this._id
  }

  _commit () {
    this._epoch++
    this._pending.forEach(pend => {
      this._data[pend.key] = this._data[pend.key] || {}
      this._data[pend.key][pend.value] = true
    })
    this._pending = []
    this._commitTimer = 0
    return null
  }

  _commitSoon () {
    if (!this._commitTimer) {
      this._commitPromise = new Promise((resolve, reject) => {
        this._commitTimer = setTimeout(() => resolve(this._commit()),
          200 * (Math.random() + 1))
      })
    }
    return this._commitPromise
  }

  rpc_get (args) {
    return this._data[args.key] || {}
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
