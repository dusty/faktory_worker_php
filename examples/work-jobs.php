<?php

require __DIR__ . '/../vendor/autoload.php';

use BaseKit\Faktory\FaktoryClient;
use BaseKit\Faktory\FaktoryWorker;

$client = new FaktoryClient('tcp://faktory:MYPASSWORD@localhost:7419');
$worker = new FaktoryWorker($client, ['default']);
$worker->register('MyJob', function ($job) {
  echo json_encode($job, JSON_PRETTY_PRINT) . "\n";
});
$worker->run(true);
