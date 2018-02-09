<?php
require __DIR__ . '/../vendor/autoload.php';

use BaseKit\Faktory\FaktoryClient;
use BaseKit\Faktory\FaktoryJob;
use Ramsey\Uuid\Uuid;

$COUNT = 1000;

$client = new FaktoryClient('tcp://localhost:7419');

for ($i = 0; $i < $COUNT; $i++) {
  $data = ['time' => new DateTime()];
  $job = new FaktoryJob(Uuid::uuid1(), 'MyJob', [$data]);
  $client->push($job);
}
