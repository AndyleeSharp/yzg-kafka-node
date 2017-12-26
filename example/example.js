let YZGKafkaConsumer = require('..').YZGKafkaConsumer;
let YZGKafkaProducer = require('..').YZGKafkaProducer;
let enableDebug = require('..').enableDebug;

enableDebug();
let c = new YZGKafkaConsumer('kn-kafka:2181', {
	topics: ['test'],
	autoCommit:false	
});

c.on('message', function(message) {
	console.log(message);
	c.consumerComit(true);
})


let p = new YZGKafkaProducer('kn-kafka:2181')
setInterval(function() {
	p.send('test',(new Date()).toLocaleString());
}, 1000)

process.on('uncaughtException', function(err) {
	console.log(' Caught exception: ' + err.stack);

});