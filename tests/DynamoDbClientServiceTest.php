<?php

namespace Rennokki\DynamoDb\Tests;

use PHPUnit\Framework\TestCase;
use Rennokki\DynamoDb\DynamoDbClientInterface;
use Rennokki\DynamoDb\DynamoDbModel;

/**
 * Class DynamoDbClientServiceTest.
 */
class DynamoDbClientServiceTest extends TestCase
{
    public function setUp(): void
    {
        parent::setUp();

        DynamoDbModel::unsetDynamoDbClientService();
    }

    /**
     * Test that the dynamoDbClientService is not initialized without the service provider being called.
     *
     * @test
     */
    public function testGetDynamoDbClientServiceUninitialized()
    {
        $dynamoDbClientService = DynamoDbModel::getDynamoDbClientService();

        $this->assertNull(
            $dynamoDbClientService,
            'The default value of the DynamoDbClientService set on the DynamoDbModels should be null.'
        );
    }

    /**
     * Test that the getter and setter behave as expected for the DynamoDbClientService.
     *
     * @test
     */
    public function testSetGetDynamoDbClientService()
    {
        $mockClientService = $this->getMockBuilder(DynamoDbClientInterface::class)->getMock();

        DynamoDbModel::setDynamoDbClientService($mockClientService);

        $dynamoDbClientService = DynamoDbModel::getDynamoDbClientService();

        $this->assertInstanceOf(
            get_class($mockClientService),
            $dynamoDbClientService,
            'The returned DynamoDbClientService should be the same as the value provided to the setter.'
        );
    }

    /**
     * The unset method for the DynamoDbClientService should cause the internal DynamoDbClientService to be set to null.
     *
     * @test
     */
    public function testUnsetDynamoDbClientService()
    {
        DynamoDbModel::unsetDynamoDbClientService();

        $dynamoDbClientService = DynamoDbModel::getDynamoDbClientService();

        $this->assertNull(
            $dynamoDbClientService,
            'After un-setting the DynamoDbClientService on the DynamoDbModel the getter should return null.'
        );
    }

    /**
     * Make sure we are not leaving any values set on the DynamoDbModel.
     */
    public function tearDown(): void
    {
        parent::tearDown();

        DynamoDbModel::unsetDynamoDbClientService();
    }
}
