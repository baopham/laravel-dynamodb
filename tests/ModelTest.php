<?php

namespace BaoPham\DynamoDb\Tests;

use BaoPham\DynamoDb\DynamoDbServiceProvider;

/**
 * Class ModelTest
 *
 * @package BaoPham\DynamoDb\Tests
 */
abstract class ModelTest extends TestCase
{
    /**
     * @var TestModel
     */
    protected $testModel;

    public function setUp()
    {
        parent::setUp();

        $this->setUpDatabase();

        $this->testModel = $this->getTestModel();
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
        $app['config']->set('dynamodb.default', 'test');
        $app['config']->set('dynamodb.connections.test', [
            'credentials' => [
                'key' => 'dynamodb_local',
                'secret' => 'secret',
            ],
            'region' => 'test',
            'endpoint' => 'http://localhost:3000',
        ]);
    }

    abstract protected function getTestModel();

    protected function setUpDatabase()
    {
        copy(dirname(__FILE__) . '/../dynamodb_local_init.db', dirname(__FILE__) . '/../dynamodb_local_test.db');
    }

    protected function getClient()
    {
        return $this->testModel->getClient();
    }
}
