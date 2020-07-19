/**
 * project JSDoc description
 * @module {Object} module name
 * @version 1.0.0
 * @author aThotino GOBIN-GANSOU
 * @requires bluebird
 * @requires kafka-node
 * @requires fs-extra
 * @requires path
 */

"use strict";

//================================================================================
// dependencies
//================================================================================
const Promise = global.Promise = require("bluebird");
const kafka = require("kafka-node");
const fs = require("fs-extra");
const path = require("path");

//================================================================================
// config
//================================================================================
const configKafka = fs.readJsonSync(path.resolve(__dirname, "../conf/config-kafka.json"));

//================================================================================
// aliases
//================================================================================
const Producer = kafka.Producer;
const Client = kafka.KafkaClient;

//================================================================================
// module
//================================================================================
const client = new Client(configKafka.options);
const topicProducer = new Producer(client);


topicProducer.on("ready", () => { console.log("producer ready"); });
topicProducer.on("error", (error) => { throw error; });

/**
 * @function createCurrentTopic
 * @param {*} topicName - The name of the topic
 */
module.exports.createCurrentTopic = function createCurrentTopic(topicName) {
  return Promise.try(() => {
    /* const topicToCreate = [
      {
        topic: topicName,
        partitions: 1,
        replicationFactor: 1,
      },
    ];*/

    client.loadMetadataForTopics([topicName], (err, result0) => {
      if (err) { throw err; }
      console.log(result0);
    });
  });
};

/**
 * @function sendSingleRequest
 * @param {*} message - The message as an object
 * @param {*} topicName - The name of the topic
 */
module.exports.sendSingleRequest = function sendSingleRequest(message, topicName = configKafka.defaultTopic) {
  return Promise.try(() => {
    topicProducer.send([
      {
        topic: topicName,
        messages: JSON.stringify(message),
        timestamp: Date.now(),
        partition: 0,
      },
    ], (error, data) => {
      if (error) { console.error(error); }
      console.log(data);
    });
  });
};
