<?php

namespace BaoPham\DynamoDb\Testing;

use Aws\DynamoDb\Exception\DynamoDbException;
use BaoPham\DynamoDb\Facades\DynamoDb;
use Illuminate\Contracts\Support\Arrayable;
use PHPUnit\Framework\Assert;

trait DynamoDbTestAssertions
{
    public static function assertDynamoDbHasTable(string $table, ?string $connection = null)
    {
        Assert::assertTrue(
            static::dynamoDbTableExists($table, $connection),
            "Failed asserting that DynamoDB table [{$table}] exists."
        );
    }

    public static function assertDynamoDbMissingTable(string $table, ?string $connection = null)
    {
        Assert::assertFalse(
            static::dynamoDbTableExists($table, $connection),
            "Failed asserting that DynamoDB table [{$table}] does not exist."
        );
    }

    public static function assertDynamoDbHasItem(string $table, string $key, $keyValue, ?string $connection = null)
    {
        $item = static::findDynamoDbItem($table, $key, $keyValue, $connection);

        Assert::assertNotNull(
            $item,
            static::formatMissingItemMessage($table, $key, $keyValue)
        );
    }

    public static function assertDynamoDbMissingItem(string $table, string $key, $keyValue, ?string $connection = null)
    {
        $item = static::findDynamoDbItem($table, $key, $keyValue, $connection);

        Assert::assertNull(
            $item,
            static::formatUnexpectedItemMessage($table, $key, $keyValue)
        );
    }

    public static function assertDynamoDbItemEquals(
        string $table,
        string $key,
        $keyValue,
        $expected,
        ?string $connection = null
    ) {
        $item = static::findDynamoDbItem($table, $key, $keyValue, $connection);

        Assert::assertNotNull(
            $item,
            static::formatMissingItemMessage($table, $key, $keyValue)
        );

        Assert::assertEquals(static::normalizeExpectedItem($expected), $item);
    }

    public static function assertDynamoDbItemNotEquals(
        string $table,
        string $key,
        $keyValue,
        $expected,
        ?string $connection = null
    ) {
        $item = static::findDynamoDbItem($table, $key, $keyValue, $connection);

        Assert::assertNotNull(
            $item,
            static::formatMissingItemMessage($table, $key, $keyValue)
        );

        Assert::assertNotEquals(static::normalizeExpectedItem($expected), $item);
    }

    protected static function dynamoDbTableExists(string $table, ?string $connection = null): bool
    {
        try {
            DynamoDb::client($connection)->describeTable(['TableName' => $table]);

            return true;
        } catch (DynamoDbException $exception) {
            if ($exception->getAwsErrorCode() === 'ResourceNotFoundException') {
                return false;
            }

            throw $exception;
        }
    }

    protected static function findDynamoDbItem(
        string $table,
        string $key,
        $keyValue,
        ?string $connection = null
    ): ?array {
        $result = DynamoDb::client($connection)->getItem([
            'TableName' => $table,
            'Key' => DynamoDb::marshalItem([$key => $keyValue]),
            'ConsistentRead' => true,
        ]);

        $item = $result->get('Item');

        if (!is_array($item) || $item === []) {
            return null;
        }

        return DynamoDb::unmarshalItem($item);
    }

    protected static function normalizeExpectedItem($expected)
    {
        if ($expected instanceof Arrayable) {
            return $expected->toArray();
        }

        return $expected;
    }

    protected static function formatMissingItemMessage(string $table, string $key, $keyValue): string
    {
        $formattedValue = static::formatKeyValue($keyValue);

        return "Failed asserting that DynamoDB table [{$table}] has item with [{$key}] = {$formattedValue}.";
    }

    protected static function formatUnexpectedItemMessage(string $table, string $key, $keyValue): string
    {
        $formattedValue = static::formatKeyValue($keyValue);

        return "Failed asserting that DynamoDB table [{$table}] does not have item with [{$key}] = {$formattedValue}.";
    }

    protected static function formatKeyValue($keyValue): string
    {
        if (is_scalar($keyValue)) {
            return (string) $keyValue;
        }

        return (string) json_encode($keyValue);
    }
}
