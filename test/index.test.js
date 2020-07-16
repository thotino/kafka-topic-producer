"use strict";
const kafkaProd = require("../index");

const data = {
    test: "test",
    foo: "bar",
};

kafkaProd.makeProducer.sendSingleRequest("testTopic", data).then((data) => {console.log(data);});