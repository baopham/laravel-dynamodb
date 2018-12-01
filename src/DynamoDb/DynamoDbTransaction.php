<?php

namespace BaoPham\DynamoDb\DynamoDb;

use Aws\DynamoDb\DynamoDbClient;
use BaoPham\DynamoDb\DynamoDbClientInterface;
use BaoPham\DynamoDb\NotSupportedException;

/**
 * Class DynamoDbTransaction
 *
 * @package BaoPham\DynamoDb\DynamoDb
 */
class DynamoDbTransaction
{
    const WRITE = 'write';
    const READ = 'read';

    /**
     * @var DynamoDbClientInterface
     */
    private $service;

    /**
     * @var DynamoDbClient
     */
    private $client;

    /**
     * @var array
     */
    private $transactItems = [];

    public function __construct(DynamoDbClientInterface $service, DynamoDbClient $client)
    {
        $this->service = $service;
        $this->client = $client;
    }

    /**
     * @return DynamoDbTransactItemManager
     */
    public function beginTransactItem()
    {
        $manager = new DynamoDbTransactItemManager($this->service);
        $this->transactItems[] = $manager;
        return $manager;
    }

    /**
     * @param string $type
     * @return \Aws\Result
     * @throws NotSupportedException
     */
    public function commit($type)
    {
        $transactItems = array_map(function (DynamoDbTransactItemManager $manager) {
            return $manager->getQueries();
        }, $this->transactItems);

        print_r($transactItems);

        if ($type === self::WRITE) {
//            return $this->client->transactWriteItems([
//                'TransactItems' => $transactItems,
//            ]);
        }

        if ($type === self::READ) {
//            return $this->client->transactGetItems([
//                'TransactItems' => $transactItems,
//            ]);
        }

//        throw new NotSupportedException('Invalid transaction type. Only support: '.self::READ.' or '.self::WRITE);
    }
}
