<?php

require __DIR__ . '/../vendor/autoload.php';
require __DIR__ . '/vendor/autoload.php';

use BaseKit\Faktory\FaktoryClient;
use BaseKit\Faktory\FaktoryWorker;

$logger = new Monolog\Logger('worker');
$handler = new Monolog\Handler\StreamHandler('php://stdout', Monolog\Logger::DEBUG);
$logger->pushHandler($handler);

$client = new FaktoryClient('tcp://faktory:MYPASSWORD@localhost:7419', 5, ['My Label']);
$worker = new FaktoryWorker($client, ['default'], $logger);
$worker->register('MyJob', function ($job) {
  // do something
});
$worker->run(true);
