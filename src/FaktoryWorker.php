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
  private $stop = false;

  /**
   * @param FaktoryClient $client
   * @param LoggerInterface $logger
   */
  public function __construct(FaktoryClient $client, LoggerInterface $logger = null)
  {
    $this->client = $client;
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

    pcntl_signal(SIGTERM, function ($signo) {
      exit(0);
    });

    pcntl_signal(SIGINT, function ($signo) {
      exit(0);
    });

    do {
      if (!$this->lastBeat || ($this->lastBeat + 15) < time()) {
        $response = $this->client->beat();
        $this->lastBeat = time();
      }

      $job = $this->client->fetch($this->queues);

      if ($job !== null) {
        $this->logger->debug($job['jid']);

        $callable = $this->jobTypes[$job['jobtype']];

        $pid = pcntl_fork();
        if ($pid === -1) {
          throw new \Exception('Could not fork');
        }

        if ($pid > 0) {
          pcntl_wait($status);
        } else {
          try {
            call_user_func($callable, $job);
            $this->client->ack($job['jid']);
          } catch (\Exception $e) {
            $this->client->fail($job['jid'], $e);
          } finally {
            exit(0);
          }
        }
      }
      usleep(100);
    } while ($daemonize && !$this->stop);
  }

  /**
   * @param array $queues
   */
  public function setQueues(array $queues): void
  {
    $this->queues = $queues;
  }
}
