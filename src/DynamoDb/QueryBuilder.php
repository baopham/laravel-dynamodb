<?php

namespace BaoPham\DynamoDb\DynamoDb;

use Aws\DynamoDb\DynamoDbClient;
use BadMethodCallException;
use BaoPham\DynamoDb\DynamoDbClientInterface;

/**
 * Class QueryBuilder
 *
 * @package BaoPham\DynamoDb\DynamoDb
 *
 * Methods are in the form of `set<key_name>`, where `<key_name>`
 * is the key name of the query body to be sent.
 *
 * For example, to build a query:
 * [
 *     'AttributeDefinitions' => ...,
 *     'GlobalSecondaryIndexUpdates' => ...
 *     'TableName' => ...
 * ]
 *
 * Do:
 *
 * $query = $query->setAttributeDefinitions(...)->setGlobalSecondaryIndexUpdates(...)->setTableName(...);
 *
 * When ready:
 *
 * $query->prepare()->updateTable();
 *
 * Common methods:
 *
 * @method QueryBuilder setExpressionAttributeNames(array $mapping)
 * @method QueryBuilder setExpressionAttributeValues(array $mapping)
 * @method QueryBuilder setFilterExpression(string $expression)
 * @method QueryBuilder setKeyConditionExpression(string $expression)
 * @method QueryBuilder setProjectionExpression(string $expression)
 * @method QueryBuilder setUpdateExpression(string $expression)
 * @method QueryBuilder setAttributeUpdates(array $updates)
 * @method QueryBuilder setScanIndexForward(bool $forward)
 * @method QueryBuilder setExclusiveStartKey(mixed $key)
 * @method QueryBuilder setReturnValues(string $type)
 * @method QueryBuilder setTableName(string $table)
 * @method QueryBuilder setIndexName(string $index)
 * @method QueryBuilder setSelect(string $select)
 * @method QueryBuilder setItem(array $item)
 * @method QueryBuilder setLimit(int $limit)
 * @method QueryBuilder setKey(array $item)
 */
class QueryBuilder
{
    /**
     * @var DynamoDbClientInterface
     */
    private $service;

    /**
     * Query body to be sent to AWS
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
        return new ExecutableQuery($client ?: $this->service->getClient(), $this->query);
    }

    /**
     * @param  string $method
     * @param  array  $parameters
     * @return mixed
     */
    public function __call($method, $parameters)
    {
        if (starts_with($method, 'set')) {
            $this->query[str_after($method, 'set')] = current($parameters);

            return $this;
        }

        throw new BadMethodCallException(sprintf(
            'Method %s::%s does not exist.',
            static::class,
            $method
        ));
    }
}
