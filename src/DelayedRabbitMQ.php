<?php

namespace zhangv\queue\rabbitmq;

use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Message\AMQPMessage;

class DelayedRabbitMQ extends RabbitMQ {
	/** @var  AMQPChannel */
	private $channel;

	const DELAY_1_MIN = 1, DELAY_5_MIN = 5, DELAY_1_HOUR = 60;
	private $delay;

	public function __construct($host,$port,$username,$password,$queue,$exchange,$queueProperties = null,$delay = DelayedRabbitMQ::DELAY_1_MIN){
		parent::__construct($host,$port,$username,$password,$queue,$exchange,$queueProperties);
		$this->delay = $delay;
	}

	public function enqueue($data,$properties = []){
		$connection = $this->getConnection();
		$this->channel = $connection->channel();
		$this->channel->queue_declare($this->queue, false, true, false, false,false, $this->queueProperties);
		$this->channel->exchange_declare($this->exchange, 'direct', false, true, false);
		$this->channel->queue_bind($this->queue, $this->exchange);

		$delayQueue = "{$this->queue}.delay.{$this->delay}";
		$delayExchange = "{$this->queue}.delayexchange";
		$this->channel->exchange_declare($delayExchange, 'direct', false, true, false);
		$this->channel->queue_declare($delayQueue,false, true, false, false,true,
		[
			'x-message-ttl' => array('I', $this->delay * 1000),   // delay in seconds to milliseconds
//			"x-expires" => array("I", $this->delay*1000+1000),
			'x-dead-letter-routing-key' => array('S',''), //use the blank routing key
			'x-dead-letter-exchange' => array('S', $this->exchange) // after message expiration in delay queue, move message to the right.now.queue

		]);
		$this->channel->queue_bind($delayQueue, $delayExchange,$this->delay);

		$properties = array('content_type' => 'text/plain', 'delivery_mode' => AMQPMessage::DELIVERY_MODE_PERSISTENT,'timestamp' => time());
		$msg = new AMQPMessage(json_encode($data),$properties);

		$this->channel->basic_publish($msg,$delayExchange,$this->delay);
		$this->channel->close();
		return $msg;
	}

}