<?php

namespace BaoPham\DynamoDb\Tests;

/**
 * Class DynamoDbModelTest
 *
 * @package BaoPham\DynamoDb\Tests
 */
abstract class DynamoDbModelTest extends DynamoDbTestCase
{
    /**
     * @var \BaoPham\DynamoDb\DynamoDbModel
     */
    protected $testModel;

    public function setUp()
    {
        parent::setUp();

        $this->testModel = $this->getTestModel();
    }

    abstract protected function getTestModel();

    protected function getClient()
    {
        return $this->testModel->getClient();
    }
}
