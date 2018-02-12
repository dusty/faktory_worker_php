<?php
require __DIR__ . '/../vendor/autoload.php';

use BaseKit\Faktory\FaktoryClient;
use BaseKit\Faktory\FaktoryJob;

$COUNT = 1000;

$client = new FaktoryClient('tcp://me:pass@localhost:7419', ['default'], 5);

for ($i = 0; $i < $COUNT; $i++) {
  $data = ['time' => new DateTime()];
  $job = new FaktoryJob('MyJob', [$data]);
  // $job->setQueue('default');
  // $job->setPriority(5);
  // $job->setReserve(1800);
  // $job->setAt(new \DateTime());
  // $job->setRetry(25);
  // $job->setBacktrace(0)
  $client->push($job);
}
$client->close();
