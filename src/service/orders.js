'use strict';

const dbops = require('../rds/create');

/**
 * Create an order
 *
**/
exports.createOrder = function (req) {
  return new Promise(function (resolve, reject) {
    dbops.createOrder(req, (err, res) => {
      if (err) {
        if (err.type === 'rule') {
          reject({
            code: 400,
            payload: { msg: err.msg }
          })
        } else {
          reject({
            code: 500,
            payload: { msg: err.msg }
          })
        }
      } else {
        resolve({
          code: 201,
          payload: {
            poll: `/eks-saga/trail/${res.orderId}`
          }
        })
      }
    })
  });
}