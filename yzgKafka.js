let kafka = require('kafka-node');
let events = require('events');
let debug = true;
let HighLevelProducer = kafka.HighLevelProducer;
let HighLevelConsumer = kafka.HighLevelConsumer;
const timeToRetryConnection = 1000;
class YZGKafka extends events.EventEmitter {
	constructor(host, opts) {
		super();
		opts = opts || {};
		this._isClose = false;
		this.groupId = opts.groupId || 'yzg-kafka-group';
		this.host = host;
		this.reconnectTimer = null;		

	}

	_close() {
		this.client.close();
	}

	_beginRetryConnect() {
		if (!this._isClose) {
			if (this.reconnectTimer == null) { // Multiple Error Events may fire, only set one connection retry.
				this.reconnectTimer = setTimeout(() => {
					this.setupConnectionToKafka();
				}, timeToRetryConnection);
			}
		}
	}

	_init() {
		this.client = new kafka.Client(this.host);			

		this.client.on('error', err => {
			debug && console.log('=====client err======');
			debug && console.log(err);
			this._close();
			this._beginRetryConnect();
			
		});

		this.client.on('close', () => {
			debug && console.log('=====client close======');			

			this._beginRetryConnect();

		})
	}	

	setupConnectionToKafka() {
		if (this.reconnectTimer != null) {
			clearTimeout(this.reconnectTimer);
			this.reconnectTimer = null;
		}
		this._init();
	}	

	close() {
		this._isClose = true;
		this.client.close();
	}

}

class YZGKafkaProducer extends YZGKafka {
	constructor(host) {
		super(host);

		this._init();

	}

	_close() {
		super._close();
		this.producer.close(() => {});
	}


	_init() {

		super._init();


		this.producer = new HighLevelProducer(this.client);

		this.producer.on('error', err => {
			debug && console.log('=====producer err======');
			debug && console.log(err);
			this._close();
			this._beginRetryConnect();

		});

	}
	
	send(topic, message) {
		let payloads = [{
			topic: topic,
			messages: message
		}];
		this.producer.send(payloads, (err, data) => {
			if (err) {
				debug && console.log(err);
			}
		});
	}
	

}


class YZGKafkaConsumer extends YZGKafka {
	constructor(host, opts) {
		super(host,opts);		
		this.autoCommit = true;
		if ('autoCommit' in opts) {
			this.autoCommit = opts.autoCommit 
		}
		
		let topics = opts.topics || [];
		this.topics = topics.map(t => {
			return {
				topic: t
			}
		});
		this._init();

	}

	_close() {		
		super._close();
		this.consumer.close(true, () => {});
	}

	

	_init() {
		super._init();

		this._initCounsumer();
		
	}

	_initCounsumer() {
		debug && console.log('=====_initCounsumer======');
		this.consumer = new HighLevelConsumer(this.client, this.topics, {
			groupId: this.groupId,
			autoCommit: this.autoCommit
		});

		this.consumer.on('message', message => {
			debug && console.log('====message======', this.consumer.id);
			this.emit('message', message);
		});

		this.consumer.on('error', err => {
			debug && console.log('====consumer err======', this.consumer.id);
			debug && console.log(err);

			this._close();
			this._beginRetryConnect();

		})

	}	

	consumerComit(force, cb) {
		if (arguments.length === 1) {
			cb = force;
			force = false;
		}

		this.consumer.commit(force, msg => {});
	}	

}

function enableDebug() {
	debug = true;
}

function disenableDebug() {
	debug = false;
}

module.exports.YZGKafkaConsumer = YZGKafkaConsumer;
module.exports.YZGKafkaProducer = YZGKafkaProducer;
module.exports.enableDebug = enableDebug;
module.exports.disenableDebug = disenableDebug;

disenableDebug();

let k = new YZGKafkaConsumer('kn-kafka:2181', {
	topics: ['test'],
	autoCommit:false	
});

k.on('message', function(message) {
	console.log(message);
	k.consumerComit(true);
})


let p = new YZGKafkaProducer('kn-kafka:2181')
setInterval(function() {
	p.send('test',(new Date()).toLocaleString());
}, 1000)

process.on('uncaughtException', function(err) {
	console.log(' Caught exception: ' + err.stack);

});