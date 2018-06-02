<?php
use zhangv\queue\rabbitmq\RabbitMQ;

class RabbitMQTest extends \PHPUnit\Framework\TestCase {
	/** @var RabbitMQ */
	private $rmq = null;

	public function setUp(){
		$cfg =  [
			'host' => 'x.x.x.x',
			'port' => '5672',
			'username' => 'user',
			'password' => 'password',
			'virtualHost' => '/'
		];
		$this->rmq = new RabbitMQ($cfg['host'],$cfg['port'],$cfg['username'],$cfg['password'],'test.queue','test.exchange');
		$queueProps = array(
			'x-message-ttl'=>array('I',5000),
			'x-dead-letter-exchange'=>array('S','test.dlexchange'),
			'x-expires' => array('I',60000) //How long a queue can be unused for before it is automatically deleted (milliseconds).
		);
		$this->rmq->setQueueProperties($queueProps);
		$this->rmq->delete();
		$this->rmq->deadLettered = true;
		$this->rmq->deadLetterExchange = 'twhtest.dlexchange';
		$this->rmq->deadLetterQueue = 'twhtest.dlqueue';
		$this->rmq->initDeadLetterQueue();
	}

	public function testEnqueue(){
		$val = time();
		$data = ['a'=> $val];
		$this->rmq->enqueue($data,[]);
		$this->rmq->setTimeout(1);
		$this->rmq->dequeue(function($message) use ($val){
			$body = json_decode($message->body);
			$this->assertEquals($val,$body->a);
			$message->delivery_info['channel']->basic_ack($message->delivery_info['delivery_tag']);
		});
	}

	public function testDequeueException(){
		$val = time();
		$data = ['a'=> $val];
		$this->rmq->enqueue($data,[]);
		$this->rmq->setTimeout(1);
		$this->rmq->dequeue(function($message) use ($val){
			try{
				throw new \Exception('fake exception');
			}catch (Exception $e){
				var_dump($e->getMessage());
				$message->delivery_info['channel']->basic_nack($message->delivery_info['delivery_tag'],false,true);
			}

		});
	}

	public function testDLQueue(){
		$val = time();
		$data = ['a'=> $val];
		$this->rmq->enqueue($data,[]);
		$this->rmq->setTimeout(1);
		$this->rmq->dequeue(function($message) use ($val){
			$message->delivery_info['channel']->basic_reject($message->delivery_info['delivery_tag'],true);
			return;
		});
	}

}
