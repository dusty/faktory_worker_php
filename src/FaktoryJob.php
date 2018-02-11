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
   * @var string
   */
  private $id;

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

  public function jsonSerialize(): array
  {
    return [
      'jid' => $this->id,
      'jobtype' => $this->type,
      'args' => $this->args,
    ];
  }
}
