<?php
use MongoToMysql\Porter;
use Garden\Cli\Cli;
use Garden\Cli\Schema;

error_reporting(E_ALL); //E_ERROR | E_PARSE | E_CORE_ERROR | E_COMPILE_ERROR | E_USER_ERROR | E_RECOVERABLE_ERROR);
ini_set('display_errors', 'on');
ini_set('track_errors', 1);

date_default_timezone_set('America/Montreal');

require_once __DIR__.'/../vendor/autoload.php';
Garden\Event::fire('bootstrap');

$cli = new Cli();

$cli->description('Exports a mongodb database to mysql.')
    ->opt('host:h', 'The mysql database host.')
    ->opt('dbname:d', 'The mysql database name.', true)
    ->opt('port:p', 'The mysql database port.')
    ->opt('username:u', 'The mysql database username.', true)
    ->opt('password:p', 'The mysql database password.')
    ->opt('mdbname', 'The mongodb database name.', true)
    ->opt('limit', 'Limit rows to this number. This is useful for debugging very large data sets.', false);


$args = $cli->parse($argv);

$porter = new Porter($args->getOpts());
$porter->setLimit($args->getOpt('limit', 0));

try {
    $porter->run();
} catch (\Exception $ex) {
    echo $cli->red($ex->getMessage());
    return $ex->getCode();
}