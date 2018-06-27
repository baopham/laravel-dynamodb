<?php

namespace BaoPham\DynamoDb\DynamoDb;

use BaoPham\DynamoDb\RawDynamoDbQuery;

/**
 * Class ExecutableQuery
 *
 * @package BaoPham\DynamoDb\DynamoDb
 *
 * @see Connection
 */
class ExecutableQuery
{
    /**
     * @var Connection
     */
    private $connection;

    /**
     * @var RawDynamoDbQuery
     */
    private $raw;

    public function __construct(Connection $connection, RawDynamoDbQuery $raw)
    {
        $this->connection = $connection;
        $this->raw = $raw;
    }

    /**
     * @param  string $method
     * @param  array  $parameters
     * @return mixed
     */
    public function __call($method, $parameters)
    {
        return $this->connection->{$method}($this->raw);
    }
}
