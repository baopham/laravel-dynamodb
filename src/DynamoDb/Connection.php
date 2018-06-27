<?php

namespace BaoPham\DynamoDb\DynamoDb;

use Aws\DynamoDb\DynamoDbClient;
use BaoPham\DynamoDb\RawDynamoDbQuery;

class Connection
{
    /**
     * @var DynamoDbClient
     */
    public $client;

    /**
     * @var bool
     */
    public $debug;

    public function __construct(DynamoDbClient $client, $debug = false)
    {
        $this->client = $client;
        $this->debug = $debug;
    }

    public function debug($on)
    {
        $this->debug = $on;

        return $this;
    }

    public function table($table)
    {
        return $this->newQuery()->setTableName($table);
    }

    public function newQuery()
    {
        return new QueryBuilder($this);
    }

    /**
     * @param  string $method
     * @param  array  $parameters
     * @return mixed
     */
    public function __call($method, $parameters)
    {
        $param = current($parameters);

        if ($param instanceof RawDynamoDbQuery) {
            if ($this->debug) {
                return $param;
            }
            return $this->client->{$method}($param->query);
        }
    }
}
