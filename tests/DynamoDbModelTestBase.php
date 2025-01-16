<?php

namespace BaoPham\DynamoDb\Tests;

use Aws\DynamoDb\Marshaler;

/**
 * Class DynamoDbModelTest
 *
 * @package BaoPham\DynamoDb\Tests
 */
abstract class DynamoDbModelTestBase extends DynamoDbTestCase
{
    /**
     * @var \BaoPham\DynamoDb\DynamoDbModel
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
