<?php

namespace BaseKit\Faktory;

use Ramsey\Uuid\Uuid;

class FaktoryClient
{
  const EOL = "\r\n";

  const VERSION = 2;

  /**
   * @var socket
   */
  private $socket;

  /**
   * @param string $url
   * @param int $timeout
   */
  public function __construct(string $url, int $timeout = 5, $labels = [])
  {
    $this->labels = $labels;
    $this->wid = Uuid::uuid4();
    $this->connect(parse_url($url), $timeout);
  }

  /**
   * @param string $jobId
   */
  public function ack(string $jobId)
  {
    $this->write('ACK', json_encode(['jid' => $jobId]));
  }

  public function beat()
  {
    $resp = $this->write('BEAT', json_encode(['wid' => $this->wid]));
    return ($resp === 'OK') ? null : $resp;
  }

  /**
   * @return null
   */
  public function close()
  {
    if (!$this->socket) {return;}
    fwrite($this->socket, 'END');
    @fclose($this->socket);
  }

  /**
   * @param string $jobId
   */
  public function fail(string $jobId, $err = '')
  {
    $payload = ['jid' => $jobId];
    if ($err instanceof Exception) {
      $payload['errType'] = get_class($err);
      $payload['message'] = $err->getMessage();
      $payload['backtrace'] = $err->getTrace();
    } else {
      $payload['errType'] = 'Error';
      $payload['message'] = (string) $err;
    }
    $this->write('FAIL', json_encode($payload));
  }

  /**
   * @param array $queues
   * @return mixed
   */
  public function fetch(array $queues)
  {
    return $this->write('FETCH', implode(' ', $queues));
  }

  public function flush()
  {
    $this->write('FLUSH');
  }

  public function info()
  {
    $this->write('INFO');
  }

  /**
   * @param FaktoryJob $job
   */
  public function push(FaktoryJob $job)
  {
    $this->write('PUSH', json_encode($job));
  }

  /**
   * @return mixed
   */
  private function connect($url, $timeout)
  {
    $host = $url['host'];
    $port = $url['port'];
    $this->socket = stream_socket_client("tcp://${host}:$port}", $errno, $errstr, $timeout, STREAM_CLIENT_CONNECT);

    if (!$this->socket) {
      throw new \Exception("Connection Error: $errstr");
    }

    $resp = $this->read();

    if (substr($resp, 0, 2) !== 'HI') {
      throw new \Exception('Hi not received :(');
    }

    // decode the rest of the HI message
    $hi = json_decode(trim(substr($resp, 3)), true);

    $server = (int) $hi['v'];
    $client = self::VERSION;

    if ($server > $client) {
      echo "WARNING: Faktory Protocol Mismatch. Upgrade recommended\n";
      echo "  Server Version: $server\n  Client Version: $client\n\n";
    }

    $nonce = (string) @$hi['s'];
    $iterations = (int) @$hi['i'];
    $password = (string) @$url['pass'];

    $pwdhash = $this->hashPassword($nonce, $iterations, $password);

    // do something if auth is required
    $payload = [
      'wid' => $this->wid,
      'v' => self::VERSION,
      'hostname' => gethostname(),
      'pid' => getmypid(),
      'labels' => $this->labels,
    ];
    if (!empty($pwdhash)) {
      $payload['pwdhash'] = $pwdhash;
    }

    $resp = $this->write('HELLO', json_encode($payload));

    if ($resp !== 'OK') {
      throw new \Exception('OK Not Received');
    }
  }

  /**
   * @return mixed
   */
  private function fgets()
  {
    $resp = '';
    while (!strpos($resp, self::EOL)) {
      $resp .= fgets($this->socket, 4096);
    }
    return $resp;
  }

  /**
   * @param $nonce
   * @param $iterations
   * @param $password
   */
  private function hashPassword(string $nonce, int $iterations, string $password = null)
  {
    if (empty($password)) {
      throw new \Exception('Password Required');
    }
    $data = $password . $nonce;
    for ($i = 0; $i < $iterations; $i++) {
      $data = hash('sha256', $data, true);
    }
    return bin2hex($data);
  }

  /**
   * @return mixed
   */
  private function read()
  {
    $resp = $this->fgets();
    $chr = substr($resp, 0, 1);
    if ($chr === '+') {
      return trim(substr($resp, 1));
    } else if ($chr === '$') {
      $count = trim(substr($resp, 1));
      if ($count < 1) {return null;}
      return json_decode($this->fgets(), true);
    } else if ($chr === '-') {
      throw new \Exception(substr($resp, 1));
    } else {
      throw new \Exception(trim($resp));
    }
  }

  /**
   * @param string $command
   * @param string $payload
   * @return mixed
   */
  private function write(string $command, string $payload = null)
  {
    $buffer = $command . ' ' . $payload . self::EOL;
    fwrite($this->socket, $buffer);
    return $this->read();
  }
}
