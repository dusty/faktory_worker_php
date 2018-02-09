<?php

namespace BaseKit\Faktory;

class FaktoryClient
{
  /**
   * @var socket
   */
  private $socket;

  /**
   * @param string $url
   * @param int $timeout
   */
  public function __construct(string $url, int $timeout = 5)
  {
    $this->timeout = $timeout;
    $this->url = parse_url($url);
  }

  /**
   * @param string $jobId
   */
  public function ack(string $jobId)
  {
    $this->writeLine('ACK', json_encode(['jid' => $jobId]));
  }

  public function beat()
  {
    $response = $this->writeLine('BEAT', '{"wid":"foo"}');
    return (strpos($response, 0, 3) === '+OK') ? null : json_decode($response);
  }

  public function close()
  {
    if ($this->socket) {@fclose($this->socket);}
  }

  /**
   * @return mixed
   */
  public function connect()
  {
    if ($this->socket) {return;}
    $conn = $this->url['scheme'] . '://' . $this->url['host'] . ':' . $this->url['port'];
    $this->socket = stream_socket_client($conn, $errno, $errstr, $this->timeout, STREAM_CLIENT_CONNECT);
    if (!$this->socket) {
      throw new \Exception("Connect Error: $errno - $errstr");
    }

    $response = $this->readLine();
    if (substr($response, 0, 3) !== '+HI') {
      throw new \Exception('Hi not received :(');
    }

    $params = json_decode(trim(substr($response, 4, strpos($response, PHP_EOL))));
    $this->login($params);
  }

  /**
   * @param string $jobId
   */
  public function fail(string $jobId)
  {
    $this->writeLine('FAIL', json_encode(['jid' => $jobId]));
  }

  /**
   * @param array $queues
   * @return mixed
   */
  public function fetch(array $queues)
  {
    $response = $this->writeLine('FETCH', implode(' ', $queues));
    $char = $response[0];
    if ($char === '$') {
      $count = trim(substr($response, 1, strpos($response, PHP_EOL)));
      if ($count < 1) {return null;}
      $data = $this->readLine();
      return json_decode($data, true);
    }
  }

  /**
   * @param FaktoryJob $job
   */
  public function push(FaktoryJob $job)
  {
    $this->writeLine('PUSH', json_encode($job));
  }

  /**
   * @param array $params
   * @return mixed
   */
  private function login($params = [])
  {
    // DO SOMETHING WITH PARAMS (eg: Authenticate)
    $resp = $this->writeLine('HELLO', '{"wid":"foo","v":2}');
    // validate data
    if (substr($resp, 0, 3) !== '+OK') {
      throw new \Exception('OK Not Received');
    }
    return $resp;
  }

  /**
   * @return mixed
   */
  private function readLine()
  {
    $bytes = '';
    while (!strpos($bytes, PHP_EOL)) {
      $bytes .= fgets($this->socket, 1024);
    }
    return $bytes;
  }

  /**
   * @param string $command
   * @param string $json
   * @return mixed
   */
  private function writeLine(string $command, string $json)
  {
    $buffer = $command . ' ' . $json . PHP_EOL;
    fwrite($this->socket, $buffer);
    return $this->readLine();
  }
}
