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
   * @var bool
   */
  private $isQuiet = false;

  /**
   * @var bool
   */
  private $isWorking = false;

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
   * @param FaktoryClient $client
   * @param LoggerInterface $logger
   */
  public function __construct(FaktoryClient $client, array $queues = ['default'], LoggerInterface $logger = null)
  {
    $this->client = $client;
    $this->setQueues($queues);
    $this->setLogger($logger);
  }

  /**
   * @param array $args
   */
  public function quiet()
  {
    $this->isQuiet = true;
    $this->logger->info('QUIET: stopping fetch');
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

  /**
   * @param LoggerInterface $logger
   */
  public function setLogger(LoggerInterface $logger = null)
  {
    if (empty($logger)) {
      $this->logger = new \Psr\Log\NullLogger();
    } else {
      $this->logger = $logger;
    }
  }

  /**
   * @param array $queues
   */
  public function setQueues(array $queues = ['default'])
  {
    $this->queues = $queues;
  }

  /**
   * If there is a running worker here, its from a kill signal
   */
  public function terminate()
  {
    $this->logger->info('TERMINATE: shutting down worker');
    if ($this->isWorking) {
      $this->client->fail($this->isWorking, 'Forced Termination');
    }
    $this->client->close();
    exit(0);
  }

  /**
   * @return null
   */
  private function runJob()
  {
    if ($this->isQuiet) {return;}
    $job = $this->client->fetch($this->queues);
    if (empty($job)) {return;}
    $this->isWorking = $job['jid'];
    $callable = $this->jobTypes[$job['jobtype']];
    $this->logger->debug('FETCH', $job);
    try {
      call_user_func($callable, $job);
      $this->client->ack($job['jid']);
      $this->logger->info('ACK', [
        'jobid' => $job['jid'], 'jobtype' => $job['jobtype'],
      ]);
    } catch (\Exception $e) {
      $this->client->fail($job['jid'], $e);
      $this->logger->error('FAIL', [
        'jobid' => $job['jid'], 'jobtype' => $job['jobtype'], 'error' => (string) $e,
      ]);
    } finally {
      $this->isWorking = false;
    }
  }

  /**
   * @return null
   */
  private function sendBeat()
  {
    if (!$this->lastBeat || ($this->lastBeat + 15) < time()) {
      $resp = $this->client->beat();
      $this->lastBeat = time();
      $this->logger->debug('BEAT', $resp ?: []);
      if (!isset($resp['state'])) {return;}
      if ($resp['state'] === 'quiet') {
        $this->quiet();
      } else if ($resp['state'] === 'terminate') {
        $this->terminate();
      }
    }
  }
}
