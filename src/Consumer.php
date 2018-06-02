<?php
namespace zhangv\queue\rabbitmq;
//ref:https://github.com/php-amqplib/php-amqplib/blob/master/demo/amqp_consumer_signals.php
//ref:https://github.com/php-amqplib/RabbitMqBundle/blob/master/RabbitMq/Consumer.php
use PhpAmqpLib\Connection\AMQPStreamConnection;
use PhpAmqpLib\Message\AMQPMessage;
use PhpAmqpLib\Exception\AMQPTimeoutException;
use Monolog\Logger;
use Monolog\Handler\StreamHandler;
use Monolog\Formatter\LineFormatter;

class Consumer{
	protected $ttl = 60*60;//1hour
	protected $maxConsumers = 2;
	protected $startTime = null;
	protected $timeout = 60; //1min timeout

	protected $exchange = 'router';
	protected $queue    = 'default';
	protected $consumerTag = 'consumer';
	protected $dlq = 'dlq';

	protected $connection = null;
	protected $channel = null;
	protected $uniqueId = null;
	protected $pid = null;

	protected $consumed = 0;
	protected $maxConsumed = 100;
	/** @var Logger */
	protected $logger;

	public function __construct($consumerTag = null,$queue = null,$exchange = null){
		$this->startTime = time();
		$this->uniqueId = uniqid();
		if($consumerTag) $this->consumerTag = $consumerTag;
		if($queue) $this->queue = $queue;
		if($exchange) $this->exchange = $exchange;
		$this->logger();
		$this->pid = getmypid();
		$this->logger->debug("pid = {$this->pid}");

		if (extension_loaded('pcntl')) {
			declare(ticks = 1);
			define('AMQP_WITHOUT_SIGNALS', false);

			pcntl_signal(SIGTERM, [$this, 'signalHandler']);
			pcntl_signal(SIGHUP, [$this, 'signalHandler']);
			pcntl_signal(SIGINT, [$this, 'signalHandler']);
			pcntl_signal(SIGQUIT, [$this, 'signalHandler']);
			pcntl_signal(SIGUSR1, [$this, 'signalHandler']);
			pcntl_signal(SIGUSR2, [$this, 'signalHandler']);
			pcntl_signal(SIGALRM, [$this, 'alarmHandler']);
		} else {
			$this->logger->info( 'Unable to process signals.');
			exit(1);
		}

		$config = Config::item("queue.rabbitmq");
		$this->connection = new AMQPStreamConnection( $config['host'], $config['port'], $config['username'], $config['password'], '/');
	}

	/**
	 * Signal handler
	 * @param  int $signalNumber
	 * @return void
	 */
	public function signalHandler($signalNumber){
		$this->logger->info( 'Handling signal: #' . $signalNumber);
		global $consumer;

		switch ($signalNumber) {
			case SIGTERM:  // 15 : supervisor default stop
			case SIGQUIT:  // 3  : kill -s QUIT
				$consumer->stopHard();
				break;
			case SIGINT:   // 2  : ctrl+c
				$consumer->stop();
				break;
			case SIGHUP:   // 1  : kill -s HUP
				$consumer->restart();
				break;
			case SIGUSR1:  // 10 : kill -s USR1
				// send an alarm in 1 second
				pcntl_alarm(1);
				break;
			case SIGUSR2:  // 12 : kill -s USR2
				// send an alarm in 10 seconds
				pcntl_alarm(10);
				break;
			default:
				break;
		}
		return;
	}

	/**
	 * Alarm handler
	 *
	 * @param  int $signalNumber
	 * @return void
	 */
	public function alarmHandler($signalNumber){
		$this->logger->info( 'Handling alarm: #' . $signalNumber);
		$this->logger->info( memory_get_usage(true));
		return;
	}

	/**
	 * Message handler
	 * @param  PhpAmqpLib\Message\AMQPMessage $message
	 * @return void
	 * @throws Exception
	 */
	public function messageHandler(AMQPMessage $message){
		$this->logger->debug("start handling message");
		if (in_array($message->body,['stop','quit'])){
			$message->delivery_info['channel']->basic_cancel($message->delivery_info['consumer_tag']);
			$this->logger->warn("consumer quit");
		}
		$delayed = time() - $message->get('timestamp');
		$this->logger->debug("delayed - $delayed");

		$start = time();
		try{
			$this->callback($message);
		}catch (Exception $e){
			if($this->dlq){
				$this->dlq($message,$e->getMessage());
			}
//			throw $e; //这里应该处理掉，而不是再抛出
		}
		$elapse = time() - $start;
		$message->delivery_info['channel']->basic_ack($message->delivery_info['delivery_tag']);
		$this->consumed ++;
		$this->logger->debug("finish handling message, elapsed - $elapse seconds");
		// stop consuming when ttl is reached
		if ( ($this->startTime + $this->ttl) < time()) {
			$this->channel->basic_cancel($message->delivery_info['consumer_tag']);
			$this->logger->debug("consumer quit after {$this->ttl} seconds");
			$this->stopHard();
		}
	}

	public function callback(AMQPMessage $message){
		$this->logger->info( "\n--------\n");
		$this->logger->info( $message->body);
		$this->logger->info( "\n--------\n");
	}

	public function dlq($message,$expmsg){
		$this->channel->queue_declare($this->dlq, false, true, false, false);
		$this->channel->exchange_declare($this->exchange, 'direct', false, true, false);
		$this->channel->queue_bind($this->dlq, $this->exchange);
		$properties = array('content_type' => 'text/plain', 'delivery_mode' => AMQPMessage::DELIVERY_MODE_PERSISTENT,'timestamp' => time());
		$pubresult = $this->channel->basic_publish($message, $this->exchange);
	}

	/**
	 * Start a consumer on an existing connection
	 *
	 * @return void
	 */
	public function start(){
		$this->logger->debug( "Starting consumer({$this->uniqueId}).");
		$this->channel = $this->connection->channel();
		list(,,$consumerCount) = $this->channel->queue_declare($this->queue, false, true, false, false);
		if ($consumerCount >= $this->maxConsumers) {//避免太多的consumer
			$this->logger->debug( "Too many consumers({$this->maxConsumers}), exit.");
			exit;
		}

		$this->channel->exchange_declare($this->exchange, 'direct', false, true, false);
		$this->channel->queue_bind($this->queue, $this->exchange);
		$this->channel->basic_consume(
			$this->queue,
			$this->consumerTag,
			false,
			false,
			false,
			false,
			[$this,'messageHandler']
//			null,
//			['x-cancel-on-ha-failover' => ['t', true]] // fail over to another node
		);
		$this->channel->basic_qos(null, 1, null);
		$this->logger->debug( 'Enter wait.');
		while (count($this->channel->callbacks)) {
			try{
				$this->channel->wait(null, false, $this->timeout);
			}catch (AMQPTimeoutException $e){
				$this->logger->info("connection timeout after {$this->timeout} seconds");
				$this->stopHard();
				break;
			}catch (Exception $e2){//NOTE: if the exception is not caught here, signalhandler will not work
				$this->logger->warn($e2->getMessage());
				break;
			}
		}
		$this->logger->debug( 'Exit wait.');
	}

	/**
	 * Restart the consumer on an existing connection
	 */
	public function restart(){
		$this->logger->info( 'Restarting consumer.');
		$this->stopSoft();
		$this->start();
	}

	/**
	 * Close the connection to the server
	 */
	public function stopHard(){
		$this->logger->info( 'Stopping consumer by closing connection.' );
		$lifetime = time() - $this->startTime;
		$lifetime_min = $lifetime / 60;
		$speed = $this->consumed / $lifetime_min;
		$this->logger->info("Consumed {$this->consumed} messages in {$lifetime_min} minutes, {$speed}/min.");
		$this->connection->close();
	}

	/**
	 * Close the channel to the server
	 */
	public function stopSoft(){
		$this->logger->info( "Stopping consumer by closing channel." );
		$lifetime = time() - $this->startTime;
		$lifetime_min = $lifetime / 60;
		$speed = $this->consumed / $lifetime_min;
		$this->logger->info("Consumed {$this->consumed} messages in {$lifetime_min} minutes, {$speed}/min.");
		$this->channel->close();
	}

	/**
	 * Tell the server you are going to stop consuming
	 * It will finish up the last message and not send you any more
	 */
	public function stop(){
		$this->logger->info( 'Stopping consumer by cancel command.');
		// this gets stuck and will not exit without the last two parameters set
		$this->channel->basic_cancel($this->consumerTag, false, true);
	}

	private function logger(){
		if(!$this->logger){
			$this->logger = new Logger(get_class($this) .'logger');
			$this->logger->pushHandler(new StreamHandler('php://stdout', Logger::DEBUG));
		}
		return $this->logger;
	}

}