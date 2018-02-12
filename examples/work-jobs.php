<?php

require __DIR__ . '/../vendor/autoload.php';
require __DIR__ . '/vendor/autoload.php';

use BaseKit\Faktory\FaktoryClient;
use BaseKit\Faktory\FaktoryWorker;

// Init a logger
$logger = new Monolog\Logger('worker');
$handler = new Monolog\Handler\StreamHandler('php://stdout', Monolog\Logger::DEBUG);
$logger->pushHandler($handler);

// Init FaktoryClient
$client = new FaktoryClient('tcp://faktory:MYPASSWORD@localhost:7419', ['My Worker'], 5);

// Init Faktory Worker
$worker = new FaktoryWorker($client, ['default', 'other'], $logger);

// Register MyJob Handler
$worker->register('MyJob', function ($job) {
  // do something
});

// Register MyOtherJob Handler
$worker->register('MyOtherJob', function ($job) {
  // do something else
});

// Run
$worker->run(true);
