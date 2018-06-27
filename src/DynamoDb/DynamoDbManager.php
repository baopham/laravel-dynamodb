<?php

namespace BaoPham\DynamoDb\DynamoDb;

use Aws\DynamoDb\Marshaler;
use BaoPham\DynamoDb\DynamoDbClientInterface;

/**
 * Class DynamoDb
 *
 * @package BaoPham\DynamoDb\DynamoDb
 */
class DynamoDbManager
{
    /**
     * @var DynamoDbClientInterface
     */
    private $wrapper;

    /**
     * The active client instances.
     *
     * @var array
     */
    private $clients = [];

    /**
     * @var Marshaler
     */
    public $marshaler;

    public function __construct(DynamoDbClientInterface $wrapper)
    {
        $this->wrapper = $wrapper;
        $this->marshaler = $wrapper->getMarshaler();
    }

    public function marshalItem($item)
    {
        return $this->marshaler->marshalItem($item);
    }

    public function marshalValue($value)
    {
        return $this->marshaler->marshalValue($value);
    }

    public function unmarshalItem($item)
    {
        return $this->marshaler->unmarshalItem($item);
    }

    public function unmarshalValue($value)
    {
        return $this->marshaler->unmarshalValue($value);
    }

    /**
     * @param string|null $connection
     * @return \Aws\DynamoDb\DynamoDbClient
     */
    public function client($connection = null)
    {
        $connection = $connection ?: config('dynamodb.default');

        if (! isset($this->clients[$connection])) {
            $this->clients[$connection] = $this->wrapper->getClient($connection);
        }

        return $this->clients[$connection];
    }

    /**
     * @return QueryBuilder
     */
    public function newQuery()
    {
        return new QueryBuilder($this->client());
    }

    /**
     * @param string $table
     * @return QueryBuilder
     */
    public function table($table)
    {
        return $this->newQuery()->setTableName($table);
    }
}
