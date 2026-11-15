<?php

namespace BaoPham\DynamoDb\Tests\Testing;

use BaoPham\DynamoDb\Facades\DynamoDb;
use BaoPham\DynamoDb\Testing\DynamoDbTestAssertions;
use BaoPham\DynamoDb\Tests\DynamoDbTestCase;
use PHPUnit\Framework\AssertionFailedError;

class DynamoDbTestAssertionsTest extends DynamoDbTestCase
{
    use DynamoDbTestAssertions;

    protected function getEnvironmentSetUp($app)
    {
        parent::getEnvironmentSetUp($app);

        $app['config']->set('dynamodb.default', 'test');
    }

    public function testAssertDynamoDbHasAndMissingTable()
    {
        $this->assertDynamoDbHasTable('test_model');
        $this->assertDynamoDbMissingTable('non_existent_table');

        static::assertDynamoDbHasTable('composite_test_model', 'test');
    }

    public function testAssertDynamoDbHasTableFailsWhenMissing()
    {
        $this->expectException(AssertionFailedError::class);
        $this->expectExceptionMessage('Failed asserting that DynamoDB table [non_existent_table] exists.');

        $this->assertDynamoDbHasTable('non_existent_table');
    }

    public function testAssertDynamoDbMissingTableFailsWhenPresent()
    {
        $this->expectException(AssertionFailedError::class);
        $this->expectExceptionMessage('Failed asserting that DynamoDB table [test_model] does not exist.');

        $this->assertDynamoDbMissingTable('test_model');
    }

    public function testAssertDynamoDbHasAndMissingItem()
    {
        $item = [
            'id' => 'assert-item-1',
            'name' => 'Alpha Widget',
            'count' => 7,
        ];

        DynamoDb::table('test_model')
            ->setItem(DynamoDb::marshalItem($item))
            ->prepare()
            ->putItem();

        $this->assertDynamoDbHasItem('test_model', 'id', 'assert-item-1');
        $this->assertDynamoDbMissingItem('test_model', 'id', 'missing-item-1');
    }

    public function testAssertDynamoDbHasItemFailsWhenMissing()
    {
        $this->expectException(AssertionFailedError::class);
        $this->expectExceptionMessage(
            'Failed asserting that DynamoDB table [test_model] has item with [id] = missing-item-2.'
        );

        $this->assertDynamoDbHasItem('test_model', 'id', 'missing-item-2');
    }

    public function testAssertDynamoDbMissingItemFailsWhenPresent()
    {
        DynamoDb::table('test_model')
            ->setItem(DynamoDb::marshalItem(['id' => 'assert-item-2', 'name' => 'Beta']))
            ->prepare()
            ->putItem();

        $this->expectException(AssertionFailedError::class);
        $this->expectExceptionMessage(
            'Failed asserting that DynamoDB table [test_model] does not have item with [id] = assert-item-2.'
        );

        $this->assertDynamoDbMissingItem('test_model', 'id', 'assert-item-2');
    }

    public function testAssertDynamoDbItemEqualsAndNotEquals()
    {
        $item = [
            'id' => 'assert-item-3',
            'name' => 'Gamma Widget',
            'count' => 42,
        ];

        DynamoDb::table('test_model')
            ->setItem(DynamoDb::marshalItem($item))
            ->prepare()
            ->putItem();

        $this->assertDynamoDbItemEquals('test_model', 'id', 'assert-item-3', [
            'count' => 42,
            'name' => 'Gamma Widget',
            'id' => 'assert-item-3',
        ]);

        $this->assertDynamoDbItemNotEquals('test_model', 'id', 'assert-item-3', [
            'id' => 'assert-item-3',
            'name' => 'Other Widget',
            'count' => 42,
        ]);
    }

    public function testAssertDynamoDbItemEqualsFailsOnMismatch()
    {
        DynamoDb::table('test_model')
            ->setItem(DynamoDb::marshalItem(['id' => 'assert-item-4', 'name' => 'Delta']))
            ->prepare()
            ->putItem();

        $this->expectException(AssertionFailedError::class);

        $this->assertDynamoDbItemEquals('test_model', 'id', 'assert-item-4', [
            'id' => 'assert-item-4',
            'name' => 'Different',
        ]);
    }

    public function testAssertDynamoDbItemNotEqualsFailsOnMatch()
    {
        DynamoDb::table('test_model')
            ->setItem(DynamoDb::marshalItem(['id' => 'assert-item-5', 'name' => 'Epsilon']))
            ->prepare()
            ->putItem();

        $this->expectException(AssertionFailedError::class);

        $this->assertDynamoDbItemNotEquals('test_model', 'id', 'assert-item-5', [
            'id' => 'assert-item-5',
            'name' => 'Epsilon',
        ]);
    }
}
