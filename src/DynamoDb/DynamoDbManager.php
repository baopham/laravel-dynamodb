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
    protected $wrapper;

    /**
     * The active connection instances.
     *
     * @var array
     */
    protected $connections = [];

    /**
     * @var bool
     */
    public $debug = false;

    /**
     * @var Marshaler
     */
    public $marshaler;

    public function __construct(DynamoDbClientInterface $wrapper)
    {
        $this->wrapper = $wrapper;
        $this->marshaler = $wrapper->getMarshaler();
    }

    public function debug($on = true)
    {
        $this->debug = $on;

        return $this;
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

        return $this->connections[$name]->debug($this->debug);
    }

    public function client($connection = null)
    {
        return $this->connection($connection)->client;
    }

    public function newQuery()
    {
        return $this->connection()->newQuery();
    }

    /**
     * Dynamically pass methods to the default connection.
     *
     * @param  string $method
     * @param  array  $parameters
     * @return mixed
     */
    public function __call($method, $parameters)
    {
        return $this->connection()->$method(...$parameters);
    }
}
