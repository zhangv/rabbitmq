<?php

use zhangv\queue\rabbitmq\DelayedRabbitMQ;

class DelayedRabbitMQTest extends \PHPUnit\Framework\TestCase {
	/** @var DelayedRabbitMQ */
	private $rmq = null;
	private $delay = 5;

	public function setUp(){
		$cfg =  [
			'host' => 'x.x.x.x',
			'port' => '5672',
			'username' => 'guest',
			'password' => 'guest',
			'virtualHost' => '/'
		];
		$this->rmq = new DelayedRabbitMQ($cfg['host'],$cfg['port'],$cfg['username'],$cfg['password'],'test.queue','test.exchange',null,$this->delay);
	}

	public function testEnqueue(){
		$val = time();
		$data = ['a'=> $val];
		$this->rmq->enqueue($data,[]);
		$this->rmq->dequeue(function($message) use ($val){
			$body = json_decode($message->body);
			$msgProps = $message->get_properties();
			$start = $body->a;
			$now = time();
			$elapsed = $now - $start;
			$this->assertEquals($this->delay,$elapsed);
			$message->delivery_info['channel']->basic_ack($message->delivery_info['delivery_tag']);
		});
	}

}
