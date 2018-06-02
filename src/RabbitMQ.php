<?php

namespace zhangv\queue\rabbitmq;

use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Connection\AMQPStreamConnection;
use PhpAmqpLib\Message\AMQPMessage;
use PhpAmqpLib\Exception\AMQPTimeoutException;

class RabbitMQ {
	protected $host,$port,$username,$password;
	protected $exchange;
	protected $queue;
	protected $queueProperties;
	private $timeout = 5;//seconds
	/** @var  AMQPChannel */
	private $channel;
	private static $connectionPool = [];
	public $deadLettered = false;
	public $deadLetterExchange,$deadLetterQueue;

	public function __construct($host,$port,$username,$password,$queue,$exchange,$queueProperties = null){
		$this->host = $host;
		$this->port = $port;
		$this->username = $username;
		$this->password = $password;
		$this->queue = $queue;
		$this->exchange = $exchange;
		$this->queueProperties = $queueProperties;
	}

	public function initDeadLetterQueue(){
		$connection = $this->getConnection();
		$this->channel = $connection->channel();
		$this->channel->queue_declare($this->deadLetterQueue, false, true, false, false,false);
		$this->channel->exchange_declare($this->deadLetterExchange, 'direct', false, true, false);
		$this->channel->queue_bind($this->deadLetterQueue, $this->deadLetterExchange);
		$this->channel->close();
	}

	public function setQueueProperties($queueProperties){
		$this->queueProperties = $queueProperties;
	}

	/**
	 * @return AMQPStreamConnection
	 */
	public function getConnection(){
		if(!isset(self::$connectionPool["{$this->exchange}_{$this->queue}"])){
			self::$connectionPool["{$this->exchange}_{$this->queue}"] = new AMQPStreamConnection( $this->host,$this->port,$this->username,$this->password);
		}
		return self::$connectionPool["{$this->exchange}_{$this->queue}"];
	}

	public function destroy(){
		if($this->channel) $this->channel->close();
	}

	public function delete(){
		$connection = $this->getConnection();
		$this->channel = $connection->channel();
		$this->channel->queue_delete($this->queue);
	}

	public function enqueue($data,$properties = []){
		$connection = $this->getConnection();
		$this->channel = $connection->channel();
		$this->channel->queue_declare($this->queue, false, true, false, false,false, $this->queueProperties);
		$this->channel->exchange_declare($this->exchange, 'direct', false, true, false);
		$this->channel->queue_bind($this->queue, $this->exchange);
		$properties = array('content_type' => 'text/plain', 'delivery_mode' => AMQPMessage::DELIVERY_MODE_PERSISTENT,'timestamp' => time());
		$msg = new AMQPMessage(json_encode($data),$properties);
		$this->channel->basic_publish($msg, $this->exchange);
		$this->channel->close();
		return $msg;
	}

	public function dequeue(callable $callback){
		$connection = $this->getConnection();
		$this->channel = $connection->channel();
		$this->channel->queue_declare($this->queue, false, true, false, false,false,$this->queueProperties);
		$this->channel->exchange_declare($this->exchange, 'direct', false, true, false);
		$this->channel->queue_bind($this->queue, $this->exchange);
		$this->channel->basic_consume($this->queue, '', false, false, false, false,
			$callback,
			null,
			$this->queueProperties
//			['x-cancel-on-ha-failover' => ['t', true]] // fail over to another node
		);
		while (count($this->channel->callbacks)) {
			try{
				$this->channel->wait(null, false, $this->timeout);
			}catch (AMQPTimeoutException $e){
				var_dump($e->getMessage());
				break;
			}catch (\Exception $e2){
				var_dump($e2->getMessage());
				break;
			}
		}
	}

	public function setTimeout($timeout){
		$this->timeout  = $timeout;
	}

}