<?php
declare (strict_types = 1);

namespace BaseKit\Faktory;

use Psr\Log\LoggerInterface;

class FaktoryWorker
{
  /**
   * @var FaktoryClient
   */
  private $client;

  /**
   * @var array
   */
  private $jobTypes = [];

  /**
   * @var int
   */
  private $lastBeat;

  /**
   * @var LoggerInterface
   */
  private $logger;

  /**
   * @var array
   */
  private $queues = [];

  /**
   * @var bool
   */
  private $quiet = false;

  /**
   * @var bool
   */
  private $working = false;

  /**
   * @param FaktoryClient $client
   * @param LoggerInterface $logger
   */
  public function __construct(FaktoryClient $client, array $queues, LoggerInterface $logger = null)
  {
    $this->client = $client;
    $this->queues = $queues;
    if (empty($logger)) {
      $this->logger = new \Psr\Log\NullLogger();
    } else {
      $this->logger = $logger;
    }
  }

  /**
   * @param string $jobType
   * @param $callable
   */
  public function register(string $jobType, callable $callable): void
  {
    $this->jobTypes[$jobType] = $callable;
  }

  /**
   * @param bool $daemonize
   */
  public function run(bool $daemonize = false): void
  {
    pcntl_async_signals(true);
    pcntl_signal(SIGTERM, function ($signo) {$this->terminate();});
    pcntl_signal(SIGTSTP, function ($signo) {$this->quiet();});
    pcntl_signal(SIGINT, function ($signo) {$this->terminate();});
    do {
      $this->sendBeat();
      $this->runJob();
      usleep(100);
    } while ($daemonize);
  }

  private function quiet()
  {
    $this->quiet = true;
    $this->logger->debug('Received QUIET command. Stopping FETCH.');
  }

  /**
   * @return null
   */
  private function runJob()
  {
    if ($this->quiet) {return;}
    $job = $this->client->fetch($this->queues);
    if (empty($job)) {return;}

    $this->logger->debug($job['jid']);
    $this->working = true;
    $callable = $this->jobTypes[$job['jobtype']];
    try {
      call_user_func($callable, $job);
      $this->client->ack($job['jid']);
    } catch (\Exception $e) {
      $this->client->fail($job['jid'], $e);
    } finally {
      $this->working = false;
    }
  }

  /**
   * TODO
   * Handle a response of quiet or terminate
   */
  private function sendBeat()
  {
    if (!$this->lastBeat || ($this->lastBeat + 15) < time()) {
      $response = $this->client->beat();
      $this->lastBeat = time();
    }
  }

  /**
   * TODO
   * Wait 25 seconds, send a fail for if still pending, exit.
   */
  private function terminate()
  {
    $this->logger->debug('Received TERMINATE command. Shutting down.');
    $this->client->close();
    exit(0);
  }
}
