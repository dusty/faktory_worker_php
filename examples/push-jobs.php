<?php
require __DIR__ . '/../vendor/autoload.php';

use BaseKit\Faktory\FaktoryClient;
use BaseKit\Faktory\FaktoryJob;

$COUNT = 1000;

$client = new FaktoryClient('tcp://me:pass@localhost:7419');

for ($i = 0; $i < $COUNT; $i++) {
  $data = ['time' => new DateTime()];
  $job = new FaktoryJob('MyJob', [$data]);
  $client->push($job);
}