// @boppleton industries

const express = require('express')
const WebSocket = require('ws')
const ccxt = require('ccxt')
const bitmex = new ccxt.bitmex()

let symbols = [
    'XBTUSD', 'ETHUSD',
    // 'XBTM19','XBTU19','ETHM19','ADAM19','BCHM19',
    // 'EOSM19','LTCM19','TRXM19','EOSM19','XRPM19'
]

// x750 bars/pull
let mexLength = 4

let data = [[bars = {}, ['m1', 'm5', 'h1', 'd1']],]

// get ohlc history //
(async () => {
    async function getBars(symbol, bin) {
        console.log("getting " + symbol + bin)
        let bars1

        bars1 = await bitmex.fetchOHLCV(getCCXTsymbol(symbol), bin[1] + bin[0], null, 750, {reverse: true})
        for (let i = 0; i < mexLength; i++) {
            await sleep(15000)

            let bars2 = await bitmex.fetchOHLCV(getCCXTsymbol(symbol), bin[1] + bin[0], null, 750, {
                reverse: true, partial: true, endTime: new Date(bars1[0][0]).toISOString().split('.')[0] + "Z"
            })
            bars1 = bars2.concat(bars1)
        }
        bars[symbol][bin] = bars1
    }

    await symbols.forEach(async (s) => {
        setTimeout(async () => {
            await getBars(s, 'h1')
            await sleep(20000 * Math.random())
            await getBars(s, 'm1')
            await sleep(20000 * Math.random())
            await getBars(s, 'm5')
            await sleep(20000 * Math.random())
            await getBars(s, 'd1')
            await sleep(20000 * Math.random())
        }, 20000 * Math.random())
    })

})();

// bitmex websocket //
(async () => {
    const socketMessageListener = (event) => {
        let msg = JSON.parse(event.data)

        if (msg.table === 'trade') {
            tradeMsg(msg.action, msg.data)
        }
    }
    const socketOpenListener = (event) => {
        console.log("bitmex ws open")
    }
    const socketCloseListener = (event) => {
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

        if (!symbols.find((s) => {
            return s === data[0].symbol
        })) {
            return
        }

        let total = 0
        data.forEach((t) => total += t.size)
        let price = data[data.length - 1].price

        const setBars = ['m1', 'm5', 'h1', 'd1'].forEach(bin => setBar(data[0].symbol, bin))

        async function setBar(symbol, bin) {

            let currentBar = bars[symbol][bin][bars[symbol][bin].length - 1]
            let lastbarTime = currentBar[0]
            let time = new Date(data[0].timestamp).getTime()

            if (time < lastbarTime) {
                //update current bar
                let currentBar = bars[symbol][bin][bars[symbol][bin].length - 1]

                currentBar[4] = price
                currentBar[5] += total

                if (price > currentBar[2]) {
                    currentBar[2] = price
                } else if (price < currentBar[3]) {
                    currentBar[3] = price
                }
            } else {
                //make new bar with this trade and last time + tim
                let barz = bars[symbol][bin]
                let close = barz[barz.length - 1][4]
                barz.push([lastbarTime + (bin === 'm1' ? 60000 : bin === 'm5' ? 300000 : bin === 'h1' ? 3600000 : 86400000), close, close, close, close, 0])
                barz.shift()
            }
        }
    }

})()

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

const app = express()
const port = process.env.PORT || 3001

// set paths, ex. GET /XBTUSD/m1
const setPaths = (data, paths) => {
    Object.keys(data).forEach((key) => {
        paths.forEach((p) => {
            app.get('/' + key + '/' + p, (req, res) => res.send(data[key][p] || data[key]))
        })
    })
}

data.forEach((d) => {
    setPaths(d[0], d[1])
})

app.listen(port, () => console.log(`[bopple-bars] - server started on port ${port}`));



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
    return symbol
}

const sleep = (ms) => {
    return new Promise(resolve => setTimeout(resolve, ms))
}


