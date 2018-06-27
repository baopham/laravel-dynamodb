<?php

namespace BaoPham\DynamoDb\DynamoDb;

use BadMethodCallException;
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

    public function prepare()
    {
        return new ExecutableQuery(
            $this->connection,
            with(new RawDynamoDbQuery(null, $this->query))->finalize()
        );
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
            'Method %s::%s does not exist.', static::class, $method
        ));
    }
}
