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
const Client = kafka.KafkaClient;
//================================================================================
// module
//================================================================================
const client = new kafka.KafkaClient(configKafka.options);
const topicProducer = new Producer(client);


topicProducer.on("ready", () => {console.log("producer ready")});
topicProducer.on("error", (error) => { throw error; });

module.exports.createCurrentTopic = function createCurrentTopic(topicName) {
return Promise.try(() => {
	const topicToCreate = [{
        topic: topicName,
        partitions: 1,
        replicationFactor: 1
    }];

    client.loadMetadataForTopics([topicName], (err, result0) => {
	if(err) { throw error; }
		console.log(result0);

	});
});
       
};

module.exports.sendSingleRequest = function sendSingleRequest(message, topicName = configKafka.defaultTopic) {
    return Promise.try(() => {
            topicProducer.send([{
                topic: topicName,
                messages: JSON.stringify(message),
                timestamp: Date.now(),
		partition: 0,
            }], (error, data) => {
                if(error) { throw error; }
                console.log(data);

            })


        
    });
    
};
