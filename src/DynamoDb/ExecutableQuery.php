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
     * @var array
     */
    private $query;

    public function __construct(Connection $connection, array $query)
    {
        $this->connection = $connection;
        $this->query = $query;
    }

    /**
     * @param  string $method
     * @param  array  $parameters
     * @return mixed
     */
    public function __call($method, $parameters)
    {
        $op = ucfirst($method);
        $raw = new RawDynamoDbQuery($op, $this->query);
        return $this->connection->{$method}($raw->finalize());
    }
}
