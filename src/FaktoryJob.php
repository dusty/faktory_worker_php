<?php

namespace BaseKit\Faktory;

use Ramsey\Uuid\Uuid;

class FaktoryJob implements \JsonSerializable
{
  /**
   * @var array
   */
  private $args;

  /**
   * @var datetime
   */
  private $at;

  /**
   * @var int
   */
  private $backtrace;

  /**
   * @var array
   */
  private $custom;

  /**
   * @var string
   */
  private $id;

  /**
   * @var int
   */
  private $priority;

  /**
   * @var string
   */
  private $queue;

  /**
   * @var int
   */
  private $reserve;

  /**
   * @var int
   */
  private $retry;

  /**
   * @var string
   */
  private $type;

  /**
   * @param string $id
   * @param string $type
   * @param array $args
   */
  public function __construct(string $type, array $args = [])
  {
    $this->id = Uuid::uuid1();
    $this->type = $type;
    $this->args = $args;
  }

  /**
   * @return mixed
   */
  public function jsonSerialize(): array
  {
    $arr = [
      'jid' => $this->id,
      'jobtype' => $this->type,
      'args' => $this->args ?: [],
    ];
    if (!empty($this->queue)) {
      $arr['queue'] = $this->queue;
    }
    if (is_numeric($this->priority)) {
      $arr['priority'] = $this->priority;
    }
    if (is_numeric($this->reserve)) {
      $arr['reserve_for'] = $this->reserve;
    }
    if (is_a($this->at, 'DateTime')) {
      $arr['at'] = $this->at->format(\DateTime::RFC3339_EXTENDED);
    }
    if (is_numeric($this->retry)) {
      $arr['retry'] = $this->retry;
    }
    if (is_numeric($this->backtrace)) {
      $arr['backtrace'] = $this->backtrace;
    }
    if (!empty($this->custom)) {
      $arr['custom'] = $this->custom;
    }
    return $arr;
  }

  /**
   * @param array $args
   */
  public function setArgs(array $args = [])
  {
    $this->args = $args;
  }

  /**
   * @param \DateTime $at
   */
  public function setAt(\DateTime $at)
  {
    $this->at = $at;
  }

  /**
   * @param int $backtrace
   */
  public function setBacktrace(int $backtrace)
  {
    $this->backtrace = $backtrace;
  }

  /**
   * @param array $custom
   */
  public function setCustom(array $custom)
  {
    $this->custom = $custom;
  }

  /**
   * Must 1 - 9
   * @param int $priority
   */
  public function setPriority(int $priority)
  {
    if ($priority > 0 || $priority > 9) {return;}
    $this->priority = $priority;
  }

  /**
   * @param string $queue
   */
  public function setQueue(string $queue)
  {
    $this->queue = $queue;
  }

  /**
   * Minimum of 60 seconds
   *
   * @param int $reserve
   * @return null
   */
  public function setReserve(int $reserve)
  {
    if ($reserve < 60) {return;}
    $this->reserve = $reserve;
  }

  /**
   * @param int $retry
   */
  public function setRetry(int $retry)
  {
    $this->retry = $retry;
  }
}
