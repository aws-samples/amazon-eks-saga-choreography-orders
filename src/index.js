const express = require('express')
const bp = require('body-parser')
const morgan = require('morgan')

const ordersSvc = require('./service/orders')
const logger = require('./utils/logger')
const appConfig = require('./utils/config').getAppConfig()

if (Object.keys(appConfig).length === 0) {
  logger.error(`Configuration data was not received.`)
  process.exit(1)
}

const app = express()

app.use(morgan('dev'))
app.use(bp.json())

const serverPort = process.env.PORT || 8080;

app.get('/ping', (req, res) => {
  res.status(200).json({
    msg: 'OK'
  })
})

app.post('/eks-saga/orders', (req, res) => {
  ordersSvc.createOrder({
    requestHeader: req.get('X-Amzn-Trace-Id'),
    body: req.body,
    rdsConfig: appConfig.rds,
    snsConfig: appConfig.sns
  })
    .then((resp) => {
      res.status(resp.code).json(resp.payload)
    })
    .catch((resp) => {
      res.status(resp.code).json(resp.payload)
    })
})

app.use('/', (req, res) => {
  res.status(404).json({
    msg: `${req.path} is not supported.`
  })
})

app.listen(serverPort, () => {
  logger.info(`Order microservice is up at ${serverPort}`)
})
