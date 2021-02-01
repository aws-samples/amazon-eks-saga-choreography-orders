'use strict';

const mom = require('moment-timezone')
const mysql = require('mysql2');
const AWS = require('aws-sdk');

const pub = require('../sns/pub')
const logger = require('../utils/logger')
const rint = require('../utils/rint')

const TZ = process.env.TZ || 'Asia/Kolkata'
const MILLION = Math.pow(10, 6)

function getToken(dbConfig, requestId, cb) {
  var signer = new AWS.RDS.Signer();
  signer.getAuthToken({
    region: dbConfig.region,
    hostname: dbConfig.host,
    port: dbConfig.port,
    username: dbConfig.dbuser
  }, (err, token) => {
    if (err) {
      logger.error(`Request ID: ${requestId} - Error getting token ${err.code}`)
      cb(err, null)
    } else {
      logger.info(`Request ID: ${requestId} - Obtained token.`)
      dbConfig.token = token
      cb(null, dbConfig)
    }
  })
}

function getDbConnection(dbConfig, requestId, cb) {
  var conn = mysql.createConnection({
    host: dbConfig.host,
    port: dbConfig.port,
    user: dbConfig.dbuser,
    password: dbConfig.token,
    database: dbConfig.db,
    ssl: 'Amazon RDS',
    authPlugins: {
      mysql_clear_password: () => () => Buffer.from(dbConfig.token + '\0')
    }
  });

  conn.connect((err) => {
    if (err) {
      logger.error(`Request ID: ${requestId} - Database connection failed - ${err.code} ${err.message}`)
      cb(err, null)
    } else {
      logger.info(`Request ID: ${requestId} - Database connected.`)
      cb(null, conn)
    }
  })
}

function publishMessage(snsConfig, requestId, payload, cb) {
  pub.publishMessage({
    region: snsConfig.region,
    topicArn: snsConfig.topicArn
  }, {
    msg: payload
  }, (err, res) => {
    if (err) {
      logger.error(`Request ID: ${requestId} - Message could not be published on ${snsConfig.topicArn} - ${err.message}`)
      cb({ type: 'sns', msg: `Message could not be published on ${snsConfig.topicArn} - ${err.message}` })
    } else {
      logger.info(`Request ID: ${requestId} - Message Id: ${res.msgId} published on ${snsConfig.topicArn}`)
      cb(null)
    }
  })
}

function createOrder(req, cb) {
  let body = req.body
  let requestId = req.requestHeader
  let dbConfig = req.rdsConfig
  let snsConfig = req.snsConfig

  let fakeOrderId = rint.getRandomIntInclusive(1, MILLION)
  let ts = mom().tz(TZ).format('YYYY-MM-DDTHH:mm:ss.SSS')

  if (body.order_qty > 40) {
    logger.error(`Request ID: ${requestId} - Order quantity cannot be greater than 40.`)
    publishMessage({
      region: snsConfig.region,
      topicArn: snsConfig.failureTopic
    }, requestId, {
      us: 'Orders',
      msgType: 'FAIL',
      msg: {
        ts: ts,
        requestId: requestId,
        orderId: fakeOrderId,
        type: 'rule',
        msg: `Request ID: ${requestId} - Order quantity cannot be greater than 40.`
      }
    },
      (err) => {
        if (err) {
          cb(err, null)
        } else {
          cb({ type: 'rule', msg: { requestId: requestId, message: 'Order quantity cannot be greater than 40.', poll: `/eks-saga/trail/${fakeOrderId}` } }, null)
        }
      })
  } else {
    getToken(dbConfig, requestId, (iamErr, dbToken) => {
      if (iamErr) {
        publishMessage({
          region: snsConfig.region,
          topicArn: snsConfig.failureTopic
        }, requestId, {
          us: 'Orders',
          msgType: 'FAIL',
          msg: {
            ts: ts,
            requestId: requestId,
            orderId: fakeOrderId,
            type: 'iam',
            msg: `Request ID: ${requestId} - Error obtaining token - ${iamErr.code} ${iamErr.message}`
          }
        },
          (err) => {
            if (err) {
              cb(err, null)
            } else {
              cb({ type: 'iam', msg: { requestId: requestId, message: `Error obtaining token - ${iamErr.code}`, poll: `/eks-saga/trail/${fakeOrderId}` } }, null)
            }
          })
      } else {
        getDbConnection(dbToken, requestId, (dbErr, conn) => {
          if (dbErr) {
            publishMessage({
              region: snsConfig.region,
              topicArn: snsConfig.failureTopic
            }, requestId, {
              us: 'Orders',
              msgType: 'FAIL',
              msg: {
                ts: ts,
                requestId: requestId,
                orderId: fakeOrderId,
                type: 'rds',
                msg: `Request ID: ${requestId} - Database connection failed - ${dbErr.code} - ${dbErr.message}`
              }
            },
              (err) => {
                if (err) {
                  cb(err, null)
                } else {
                  cb({ type: 'rds', msg: { requestId: requestId, message: `Database connection failed - ${dbErr.code} - ${dbErr.message}`, poll: `/eks-saga/trail/${fakeOrderId}` } }, null)
                }
              })
          } else {
            let q = `INSERT INTO ${dbToken.db}.orders (order_sku_id, order_qty, order_price, order_timestamp) VALUES (?,?,?,?);`
            let v = [body.order_sku_id, body.order_qty, body.order_price, ts]

            conn.beginTransaction((err) => {
              if (err) {
                logger.error(`Request ID: ${requestId} - Error beginning transaction - ${err.code} - ${err.message}`)
                publishMessage({
                  region: snsConfig.region,
                  topicArn: snsConfig.failureTopic
                }, requestId, {
                  us: 'Orders',
                  msgType: 'FAIL',
                  msg: {
                    ts: ts,
                    requestId: requestId,
                    orderId: fakeOrderId,
                    type: 'rds',
                    msg: `Error beginning transaction - ${err.code} - ${err.message}`
                  }
                },
                  (err) => {
                    if (err) {
                      cb(err, null)
                    } else {
                      cb({ type: 'rds', msg: { requestId: requestId, message: `Error beginning transaction - ${err.code} - ${err.message}`, poll: `/eks-saga/trail/${fakeOrderId}` } }, null)
                    }
                  })
              } else {
                conn.query(q, v, (qryErr, results) => {
                  if (qryErr) {
                    conn.rollback()
                    logger.error(`Request ID: ${requestId} - Error running query - ${qryErr.code} - ${qryErr.message}`)
                    publishMessage({
                      region: snsConfig.region,
                      topicArn: snsConfig.failureTopic
                    }, requestId, {
                      us: 'Orders',
                      msgType: 'FAIL',
                      msg: {
                        ts: ts,
                        requestId: requestId,
                        orderId: fakeOrderId,
                        type: 'rds',
                        msg: `Error running query - ${qryErr.code} - ${qryErr.message}`
                      }
                    },
                      (err) => {
                        if (err) {
                          cb(err, null)
                        } else {
                          cb({ type: 'rds', msg: { requestId: requestId, message: `Error running query - ${qryErr.code} - ${qryErr.message}`, poll: `/eks-saga/trail/${fakeOrderId}` } }, null)
                        }
                      })
                  } else {
                    publishMessage({
                      region: snsConfig.region,
                      topicArn: snsConfig.successTopic
                    }, requestId, {
                      us: 'Orders',
                      msgType: 'SUCCESS',
                      msg: {
                        ts: ts,
                        requestId: `${requestId}`,
                        orderId: results.insertId,
                        orderSkuId: body.order_sku_id,
                        orderQty: body.order_qty,
                        msg: `Order created - ${results.insertId}`
                      }
                    },
                      (err) => {
                        if (err) {
                          conn.rollback()
                          cb(err, null)
                        } else {
                          conn.commit()
                          logger.info(`Request ID: ${requestId} - Order created - ${results.insertId}`)
                          cb(null, { orderId: results.insertId })
                        }
                      })
                  }
                })
              }
            })
          }
        })
      }
    })
  }
}

module.exports = {
  createOrder: createOrder
}