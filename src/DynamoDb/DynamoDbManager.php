<?php

namespace BaoPham\DynamoDb\DynamoDb;

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
    private $service;

    /**
     * @var \Aws\DynamoDb\Marshaler
     */
    public $marshaler;

    public function __construct(DynamoDbClientInterface $service)
    {
        $this->service = $service;
        $this->marshaler = $service->getMarshaler();
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
        return $this->service->getClient($connection);
    }

    /**
     * @return QueryBuilder
     */
    public function newQuery()
    {
        return new QueryBuilder($this->service);
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
