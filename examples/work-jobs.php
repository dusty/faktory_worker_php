<?php

require __DIR__ . '/../vendor/autoload.php';

use BaseKit\Faktory\FaktoryClient;
use BaseKit\Faktory\FaktoryWorker;

$logger = new Monolog\Logger('worker');
$handler = new Monolog\Handler\StreamHandler('php://stdout', Monolog\Logger::DEBUG);
$logger->pushHandler($handler);

$client = new FaktoryClient('tcp://localhost:7419');
$worker = new FaktoryWorker($client, $logger);
$worker->setQueues(['default']);
$worker->register('MyJob', function ($job) {
  echo json_encode($job['args']);
});
$worker->run(true);
