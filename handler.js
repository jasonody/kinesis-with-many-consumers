'use strict';

const aws = require('aws-sdk');
const _ = require('highland');
const uuid = require('uuid');

module.exports.producer = (event, context, callback) => {
  const streamEvent = {
    id: uuid.v1(),
    type: 'some-event',
    timestamp: Date.now(),
    item: { some: 'data' }
  };

  console.log('Producer event: %j', streamEvent);

  const params = {
    StreamName: process.env.STREAM_NAME,
    PartitionKey: uuid.v4(),
    Data: new Buffer.from(JSON.stringify(streamEvent)),
  };

  const kinesis = new aws.Kinesis();

  kinesis.putRecord(params).promise()
    .then(resp => callback(null, resp))
    .catch(err => callback(err));
}

function processor (event, context, callback) {
  _(event.Records)
    .map(mapToEvent)
    .tap(print)
    .collect()
    .toCallback(callback)
}

const mapToEvent = record => JSON.parse(new Buffer.from(record.kinesis.data, 'base64'));
const print = data => console.log('received data: %j', data)

module.exports.consumer1 = processor
module.exports.consumer2 = processor
module.exports.consumer3 = processor
module.exports.consumer4 = processor
module.exports.consumer5 = processor
module.exports.consumer6 = processor