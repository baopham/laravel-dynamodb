<?php

namespace BaoPham\DynamoDb\Tests;

use BaoPham\DynamoDb\DynamoDbServiceProvider;
use Orchestra\Testbench\TestCase;

/**
 * Class DynamoDbTestCase
 *
 * @package BaoPham\DynamoDb\Tests
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
                'key' => 'dynamodblocal',
                'secret' => 'secret',
            ],
            'region' => 'test',
            'endpoint' => env('DYNAMODB_LOCAL_ENDPOINT', 'http://localhost:3000'),
        ]);
    }

    protected function setUpDatabase()
    {
        copy(dirname(__FILE__) . '/../dynamodb_local_init.db', dirname(__FILE__) . '/../dynamodblocal_test.db');
    }
}
