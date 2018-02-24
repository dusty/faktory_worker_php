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
  private $isTerminated = false;

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
  public function __construct(FaktoryClient $client, LoggerInterface $logger = null)
  {
    $this->client = $client;
    $this->setLogger($logger);
    $this->setQueues();
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
    pcntl_signal(SIGTSTP, function () {$this->setQuiet();});
    pcntl_signal(SIGINT, function () {$this->setTerminate();});
    pcntl_signal(SIGTERM, function () {$this->setTerminate();});
    pcntl_signal(SIGALRM, function () {$this->close();});
    do {
      $this->sendBeat();
      $this->checkIsTerminated();
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
   * @return null
   */
  private function checkIsTerminated()
  {
    $timeout = $this->isTerminated ? 25 : 0;
    pcntl_alarm($timeout);
    if (!$this->isTerminated) {return;}
    if ($this->isWorking) {
      $this->logger->warning('TERMINATE: waiting 25 seconds for pending job');
    } else {
      $this->close();
    }
  }

  private function close()
  {
    if ($this->isWorking) {
      $this->logger->error('FAIL', ['jid' => $this->isWorking]);
      $this->client->fail($this->isWorking, 'Forced Termination');
    }
    $this->logger->debug('END');
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
    $this->logger->debug('FETCH', $job);
    $this->isWorking = $job['jid'];
    $callable = @$this->jobTypes[$job['jobtype']];
    try {
      if (empty($callable)) {
        throw new \Exception("Job Type Not Found: {$job['jobtype']}");
      }
      $job['args'] = $job['args'][0];
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
        $this->setQuiet();
      } else if ($resp['state'] === 'terminate') {
        $this->setTerminate();
      }
    }
  }

  private function setQuiet()
  {
    $this->isQuiet = true;
    $this->logger->warning('QUIET: stopping fetch');
  }

  private function setTerminate()
  {
    $this->isTerminated = true;
    $this->logger->warning('TERMINATE: shutting down');
    $this->checkIsTerminated();
  }
}
