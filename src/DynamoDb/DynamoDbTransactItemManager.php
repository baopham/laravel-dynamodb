<?php

namespace BaoPham\DynamoDb\DynamoDb;

use BaoPham\DynamoDb\DynamoDbClientInterface;

/**
 * Class DynamoDbTransactItemManager
 *
 * @package BaoPham\DynamoDb\DynamoDb
 */
class DynamoDbTransactItemManager
{
    /**
     * @var DynamoDbClientInterface
     */
    private $service;

    /**
     * @var DynamoDbTransactItemQueryBuilder
     */
    private $transactItemQueries;

    public function __construct(DynamoDbClientInterface $service)
    {
        $this->service = $service;
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
     * @return DynamoDbTransactItemQueryBuilder
     */
    public function table($table)
    {
        $queryBuilder = $this->newQuery()->setTableName($table);
        $query = new DynamoDbTransactItemQueryBuilder($queryBuilder);
        $this->transactItemQueries[] = $query;
        return $query;
    }

    /**
     * @return array
     */
    public function getQueries()
    {
        $queries = [];
        /** @var DynamoDbTransactItemQueryBuilder $queryBuilder */
        foreach ($this->transactItemQueries as $queryBuilder) {
            $queries[$queryBuilder->type] = $queryBuilder->query;
        }
        return $queries;
    }
}
