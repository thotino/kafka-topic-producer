/**
 * project JSDoc description
 * @module {Object} module name
 * @version 1.0.0
 * @author author name
 * @requires dependency 1
 * @requires dependency 2
 * ...
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

//================================================================================
// module
//================================================================================
const client = new kafka.KafkaClient(configKafka.options);
const topicProducer = new Producer(client);

module.exports.createCurrentTopic = function createCurrentTopic(topicName) {
    const topicToCreate = [{
        topic: topicName,
        partitions: 1,
        replicationFactor: 1
    }];
    client.loadMetadataForTopics([topicName], (err, result) => {
	if(err) {
		client.createTopics(topicToCreate, (error, result) => {
    		    if(error) {throw error}
    		});
	}
	console.log(result);	
	});    
};

module.exports.sendSingleRequest = function sendSingleRequest(message, topicName = configKafka.defaultTopic) {
    return new Promise((resolve, reject) => {
        createCurrentTopic(topicName);
        topicProducer.on("ready", () => {
            topicProducer.send([{
                topic: topicName,
                messages: JSON.stringify(message),
                timestamp: Date.now(),
            }], (error, data) => {
                if(error) { throw error; }
                console.log(data);
                return resolve(data);
            })
        });

        topicProducer.on("error", (error) => {
            return reject(error);
        });
    });
    
};
