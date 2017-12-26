# yzg-kafka-node
对kafka-node的简单包装，使之能在服务中断又恢复后继续工作。

## Consumer example
``` js
let YZGKafkaConsumer = require('yzg-kafka-node').YZGKafkaConsumer;
let consumer = new YZGKafkaConsumer('kn-kafka:2181', {
	topics: ['test'],
	autoCommit:true	
});

consumer.on('message', function(message) {
	console.log(message);	
});
```

## Producer example
``` js
let YZGKafkaProducer = require('yzg-kafka-node').YZGKafkaProducer;
let producer = new YZGKafkaProducer('kn-kafka:2181')
setInterval(function() {
	producer.send('test',(new Date()).toLocaleString());
}, 1000);
```
