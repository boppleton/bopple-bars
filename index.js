const express = require('express')
const WebSocket = require('ws')
const ccxt = require('ccxt')
const bitmex = new ccxt.bitmex()

/// @boppleton industries

let data = [
    [bars = {}, ['m1', 'm5', 'h1', 'd1']],
    [testdata = {}, ['testpath1', 'testpath2']]
]

let symbols = [
    'XBTUSD', 'ETHUSD',
    // 'XBTM19','XBTU19','ETHM19','ADAM19','BCHM19',
    // 'EOSM19','LTCM19','TRXM19','EOSM19','XRPM19'
]

let barCount = 2000


const setupDataAPI = (() => {

    // make a path for each symbol+datapath (eg. bars.XBTUSD.m1)
    const startData = (dataItem, symbols, paths) => {
        symbols.forEach((s) => {
            dataItem[s] = {}
            paths.forEach((p) => {
                dataItem[s][p] = ['[' + s + ':' + p + ':START]']
            })
        })
    }
    data.forEach((d) => {
        startData(d[0], d[2] || symbols, d[1])
    })

    // setup api server so we can add some endpoints
    const app = express()
    app.get('/', (req, res) => res.send('example path: /XBTUSD/m1'))
    app.listen(process.env.PORT || 3001)

    // set paths, ex. GET /XBTUSD/m1
    data.forEach(d => ((data, paths) =>
        Object.keys(data).forEach(key => {
            paths.forEach(p => app.get('/' + key + '/' + p, (req, res) => res.send(data[key][p] || data[key])))
        }))(d[0], d[1]))
})()


const getAllBars = (() => {

    const get = async (symbol, bin) => {
        console.log("getting " + symbol + bin)
            await new Promise(resolve => setTimeout(resolve, 5000))
        console.log(symbol+bin+'pull 1')
            let bars1 = await bitmex.fetchOHLCV(getCCXTsymbol(symbol), bin[1] + bin[0], null, 750, {reverse: true})

            for (let i = 0; i < Math.ceil(barCount / 750); i++) {
                await new Promise(resolve => setTimeout(resolve, 5000))
                console.log(symbol+bin+'pull ' +i)
                let bars2 = await bitmex.fetchOHLCV(getCCXTsymbol(symbol), bin[1] + bin[0], null, 750, {
                    reverse: true, partial: true, endTime: new Date(bars1[0][0]).toISOString().split('.')[0] + "Z"
                })
                bars1 = bars2.concat(bars1)
            }
            bars[symbol][bin] = bars1
    }

    symbols.forEach(async (symbol, i) => {

        await new Promise(resolve => setTimeout(resolve, ((barCount/1000)*40000) * i))
        console.log('get symbol ' + symbol)

        for (let j = 0; i < 4; i++) {
            await get(symbol, data[0][1][i])
        }

    })

})()


const setupBitmexWebsocket = (async () => {

    const socketMessageListener = (event) => {
        let msg = JSON.parse(event.data)

        if (msg.table === 'trade') {
            tradeMsg(msg.action, msg.data)
        }
    }

    const socketOpenListener = e => console.log('bitmex ws open')

    const socketCloseListener = e => {
        if (this.socket) {
            console.error('bitmex ws close')
        }
        this.socket = new WebSocket('wss://www.bitmex.com/realtime?subscribe=trade')
        this.socket.addEventListener('open', socketOpenListener)
        this.socket.addEventListener('message', socketMessageListener)
        this.socket.addEventListener('close', socketCloseListener)
    }
    socketCloseListener()

    const tradeMsg = (action, data) => {

        if (!symbols.find(s => s === data[0].symbol)) return

        let total = 0
        data.forEach(t => total += t.size)
        let price = data[data.length - 1].price

        const setBars = data[0][1].forEach(bin => setBar(data[0].symbol, bin))

        async function setBar(symbol, bin) {

            let bars = bars[symbol][bin]
            let currentBar = bars[bars.length - 1]
            let lastbarTime = currentBar[0]
            let time = new Date(data[0].timestamp).getTime()

            if (time < lastbarTime) {
                //update current bar
                let currentBar = bars[bars.length - 1]
                currentBar[4] = price
                currentBar[5] += total

                if (price > currentBar[2]) {
                    currentBar[2] = price
                } else if (price < currentBar[3]) {
                    currentBar[3] = price
                }
            } else {
                //make new bar with this trade and last time + tim
                let bars_ = bars
                let close = bars_[bars_.length - 1][4]
                bars_.push([lastbarTime + (bin === 'm1' ? 60000 : bin === 'm5' ? 300000 : bin === 'h1' ? 3600000 : 86400000), close, close, close, close, 0])
                bars_.shift()
            }
        }
    }
})

const getCCXTsymbol = (symbol) => {
    //mex pairs
    if (symbol === 'XBTUSD') {
        return 'BTC/USD'
    } else if (symbol === 'ETHUSD') {
        return 'ETH/USD'
    } else if (symbol.includes('M19')) {
        return symbol.replace('M19', 'M19')
    } else if (symbol.includes('U19')) {
        return symbol.replace('U19', 'U19')
    }

    //binance pairs

    if (symbol.includes('BTC')) {
        return symbol.replace('BTC', '/BTC')
    } else if (symbol.includes('USDT')) {
        return symbol.replace('USDT', '/USDT')
    }
    return symbol
}


const sleep = (ms) => {
    return new Promise(resolve => setTimeout(resolve, ms))
}
