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
     * The active connection instances.
     *
     * @var array
     */
    private $connections = [];

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
     * @param string|null $name
     * @return Connection
     */
    public function connection($name = null)
    {
        $name = $name ?: config('dynamodb.default');

        if (! isset($this->connections[$name])) {
            $this->connections[$name] = new Connection($this->wrapper->getClient($name));
        }

        return $this->connections[$name];
    }

    /**
     * @param string|null $connection
     * @return \Aws\DynamoDb\DynamoDbClient
     */
    public function client($connection = null)
    {
        return $this->connection($connection)->client;
    }

    /**
     * @return QueryBuilder
     */
    public function newQuery()
    {
        return $this->connection()->newQuery();
    }

    /**
     * @param string $table
     * @return QueryBuilder
     */
    public function table($table)
    {
        return $this->connection()->table($table);
    }
}
