<?php

namespace Rennokki\DynamoDb\Tests;

use Aws\DynamoDb\Marshaler;

/**
 * Class DynamoDbModelTest.
 */
abstract class DynamoDbModelTest extends DynamoDbTestCase
{
    /**
     * @var \Rennokki\DynamoDb\DynamoDbModel
     */
    protected $testModel;

    /**
     * @var \Aws\DynamoDb\Marshaler
     */
    protected $marshaler;

    public function setUp(): void
    {
        parent::setUp();

        $this->testModel = $this->getTestModel();
        $this->marshaler = new Marshaler();
    }

    abstract protected function getTestModel();

    protected function getClient()
    {
        return $this->testModel->getClient();
    }
}
