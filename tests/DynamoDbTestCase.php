<?php

namespace Rennokki\DynamoDb\Tests;

use Orchestra\Testbench\TestCase;
use Rennokki\DynamoDb\DynamoDbServiceProvider;

/**
 * Class DynamoDbTestCase.
 */
abstract class DynamoDbTestCase extends TestCase
{
    public function setUp(): void
    {
        parent::setUp();

        $this->setUpDatabase();
    }

    protected function getPackageProviders($app)
    {
        return [DynamoDbServiceProvider::class];
    }

    /**
     * Define environment setup.
     *
     * @param  \Illuminate\Foundation\Application  $app
     * @return void
     */
    protected function getEnvironmentSetUp($app)
    {
        $app['config']->set('dynamodb.connections.test', [
            'credentials' => [
                'key' => 'local',
                'secret' => 'secret',
            ],
            'region' => 'test',
            'endpoint' => 'http://localhost:3000',
        ]);
    }

    protected function setUpDatabase()
    {
        copy(dirname(__FILE__).'/../local_init.db', dirname(__FILE__).'/../local_test.db');
    }
}
