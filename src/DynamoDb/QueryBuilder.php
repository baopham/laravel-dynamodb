<?php

namespace Rennokki\DynamoDb\DynamoDb;

use Aws\DynamoDb\DynamoDbClient;
use BadMethodCallException;
use Illuminate\Support\Str;
use Rennokki\DynamoDb\DynamoDbClientInterface;
use Rennokki\DynamoDb\RawDynamoDbQuery;

/**
 * Class QueryBuilder.
 *
 *
 * @method QueryBuilder setExpressionAttributeNames(array $mapping)
 * @method QueryBuilder setExpressionAttributeValues(array $mapping)
 * @method QueryBuilder setFilterExpression(string $expression)
 * @method QueryBuilder setKeyConditionExpression(string $expression)
 * @method QueryBuilder setProjectionExpression(string $expression)
 * @method QueryBuilder setUpdateExpression(string $expression)
 * @method QueryBuilder setAttributeUpdates(array $updates)
 * @method QueryBuilder setConsistentRead(bool $consistent)
 * @method QueryBuilder setScanIndexForward(bool $forward)
 * @method QueryBuilder setExclusiveStartKey(mixed $key)
 * @method QueryBuilder setReturnValues(string $type)
 * @method QueryBuilder setRequestItems(array $items)
 * @method QueryBuilder setTableName(string $table)
 * @method QueryBuilder setIndexName(string $index)
 * @method QueryBuilder setSelect(string $select)
 * @method QueryBuilder setItem(array $item)
 * @method QueryBuilder setKeys(array $keys)
 * @method QueryBuilder setLimit(int $limit)
 * @method QueryBuilder setKey(array $key)
 */
class QueryBuilder
{
    /**
     * @var DynamoDbClientInterface
     */
    private $service;

    /**
     * Query body to be sent to AWS.
     *
     * @var array
     */
    public $query = [];

    public function __construct(DynamoDbClientInterface $service)
    {
        $this->service = $service;
    }

    public function hydrate(array $query)
    {
        $this->query = $query;

        return $this;
    }

    public function setExpressionAttributeName($placeholder, $name)
    {
        $this->query['ExpressionAttributeNames'][$placeholder] = $name;

        return $this;
    }

    public function setExpressionAttributeValue($placeholder, $value)
    {
        $this->query['ExpressionAttributeValues'][$placeholder] = $value;

        return $this;
    }

    /**
     * @param DynamoDbClient|null $client
     * @return ExecutableQuery
     */
    public function prepare(DynamoDbClient $client = null)
    {
        $raw = new RawDynamoDbQuery(null, $this->query);

        return new ExecutableQuery($client ?: $this->service->getClient(), $raw->finalize()->query);
    }

    /**
     * @param  string $method
     * @param  array  $parameters
     * @return mixed
     */
    public function __call($method, $parameters)
    {
        if (Str::startsWith($method, 'set')) {
            $key = array_reverse(explode('set', $method, 2))[0];
            $this->query[$key] = current($parameters);

            return $this;
        }

        throw new BadMethodCallException(sprintf(
            'Method %s::%s does not exist.',
            static::class,
            $method
        ));
    }
}
