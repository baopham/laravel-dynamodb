<?php

namespace BaoPham\DynamoDb\DynamoDb;

use BaoPham\DynamoDb\RawDynamoDbQuery;

class QueryBuilder
{
    /**
     * @var Connection
     */
    public $connection;

    /**
     * Query body to be sent to AWS
     *
     * @var array
     */
    public $query = [];

    public function __construct(Connection $connection)
    {
        $this->connection = $connection;
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

    public function scan()
    {
        $raw = new RawDynamoDbQuery('Scan', $this->query);

        return $this->connection->scan($raw->finalize());
    }

    public function query()
    {
        $raw = new RawDynamoDbQuery('Query', $this->query);

        return $this->connection->query($raw->finalize());
    }

    /**
     * @param  string $method
     * @param  array  $parameters
     * @return mixed
     */
    public function __call($method, $parameters)
    {
        if (starts_with($method, 'set')) {
            $this->query[str_after($method, 'set')] = $parameters[0];

            return $this;
        }

        $raw = new RawDynamoDbQuery(null, $this->query);

        return $this->connection->{$method}($raw->finalize());
    }
}
