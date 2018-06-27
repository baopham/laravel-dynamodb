<?php

namespace BaoPham\DynamoDb\DynamoDb;

use Aws\DynamoDb\DynamoDbClient;
use BadMethodCallException;
use BaoPham\DynamoDb\RawDynamoDbQuery;

/**
 * A proxy to DynamoDbClient
 *
 * @package BaoPham\DynamoDb\DynamoDb
 *
 * @method \Aws\Result batchGetItem(array $args = [])
 * @method \GuzzleHttp\Promise\Promise batchGetItemAsync(array $args = [])
 * @method \Aws\Result batchWriteItem(array $args = [])
 * @method \GuzzleHttp\Promise\Promise batchWriteItemAsync(array $args = [])
 * @method \Aws\Result createTable(array $args = [])
 * @method \GuzzleHttp\Promise\Promise createTableAsync(array $args = [])
 * @method \Aws\Result deleteItem(array $args = [])
 * @method \GuzzleHttp\Promise\Promise deleteItemAsync(array $args = [])
 * @method \Aws\Result deleteTable(array $args = [])
 * @method \GuzzleHttp\Promise\Promise deleteTableAsync(array $args = [])
 * @method \Aws\Result describeTable(array $args = [])
 * @method \GuzzleHttp\Promise\Promise describeTableAsync(array $args = [])
 * @method \Aws\Result getItem(array $args = [])
 * @method \GuzzleHttp\Promise\Promise getItemAsync(array $args = [])
 * @method \Aws\Result listTables(array $args = [])
 * @method \GuzzleHttp\Promise\Promise listTablesAsync(array $args = [])
 * @method \Aws\Result putItem(array $args = [])
 * @method \GuzzleHttp\Promise\Promise putItemAsync(array $args = [])
 * @method \Aws\Result query(array $args = [])
 * @method \GuzzleHttp\Promise\Promise queryAsync(array $args = [])
 * @method \Aws\Result scan(array $args = [])
 * @method \GuzzleHttp\Promise\Promise scanAsync(array $args = [])
 * @method \Aws\Result updateItem(array $args = [])
 * @method \GuzzleHttp\Promise\Promise updateItemAsync(array $args = [])
 * @method \Aws\Result updateTable(array $args = [])
 * @method \GuzzleHttp\Promise\Promise updateTableAsync(array $args = [])
 * @method \Aws\Result createBackup(array $args = []) (supported in versions 2012-08-10)
 * @method \GuzzleHttp\Promise\Promise createBackupAsync(array $args = []) (supported in versions 2012-08-10)
 * @method \Aws\Result createGlobalTable(array $args = []) (supported in versions 2012-08-10)
 * @method \GuzzleHttp\Promise\Promise createGlobalTableAsync(array $args = []) (supported in versions 2012-08-10)
 * @method \Aws\Result deleteBackup(array $args = []) (supported in versions 2012-08-10)
 * @method \GuzzleHttp\Promise\Promise deleteBackupAsync(array $args = []) (supported in versions 2012-08-10)
 * @method \Aws\Result describeBackup(array $args = []) (supported in versions 2012-08-10)
 * @method \GuzzleHttp\Promise\Promise describeBackupAsync(array $args = []) (supported in versions 2012-08-10)
 * @method \Aws\Result describeContinuousBackups(array $args = []) (supported in versions 2012-08-10)
 * @method \GuzzleHttp\Promise\Promise describeContinuousBackupsAsync(array $args = []) (supported in versions 2012-08-10)
 * @method \Aws\Result describeGlobalTable(array $args = []) (supported in versions 2012-08-10)
 * @method \GuzzleHttp\Promise\Promise describeGlobalTableAsync(array $args = []) (supported in versions 2012-08-10)
 * @method \Aws\Result describeLimits(array $args = []) (supported in versions 2012-08-10)
 * @method \GuzzleHttp\Promise\Promise describeLimitsAsync(array $args = []) (supported in versions 2012-08-10)
 * @method \Aws\Result describeTimeToLive(array $args = []) (supported in versions 2012-08-10)
 * @method \GuzzleHttp\Promise\Promise describeTimeToLiveAsync(array $args = []) (supported in versions 2012-08-10)
 * @method \Aws\Result listBackups(array $args = []) (supported in versions 2012-08-10)
 * @method \GuzzleHttp\Promise\Promise listBackupsAsync(array $args = []) (supported in versions 2012-08-10)
 * @method \Aws\Result listGlobalTables(array $args = []) (supported in versions 2012-08-10)
 * @method \GuzzleHttp\Promise\Promise listGlobalTablesAsync(array $args = []) (supported in versions 2012-08-10)
 * @method \Aws\Result listTagsOfResource(array $args = []) (supported in versions 2012-08-10)
 * @method \GuzzleHttp\Promise\Promise listTagsOfResourceAsync(array $args = []) (supported in versions 2012-08-10)
 * @method \Aws\Result restoreTableFromBackup(array $args = []) (supported in versions 2012-08-10)
 * @method \GuzzleHttp\Promise\Promise restoreTableFromBackupAsync(array $args = []) (supported in versions 2012-08-10)
 * @method \Aws\Result tagResource(array $args = []) (supported in versions 2012-08-10)
 * @method \GuzzleHttp\Promise\Promise tagResourceAsync(array $args = []) (supported in versions 2012-08-10)
 * @method \Aws\Result untagResource(array $args = []) (supported in versions 2012-08-10)
 * @method \GuzzleHttp\Promise\Promise untagResourceAsync(array $args = []) (supported in versions 2012-08-10)
 * @method \Aws\Result updateGlobalTable(array $args = []) (supported in versions 2012-08-10)
 * @method \GuzzleHttp\Promise\Promise updateGlobalTableAsync(array $args = []) (supported in versions 2012-08-10)
 * @method \Aws\Result updateTimeToLive(array $args = []) (supported in versions 2012-08-10)
 * @method \GuzzleHttp\Promise\Promise updateTimeToLiveAsync(array $args = []) (supported in versions 2012-08-10)
 */
class Connection
{
    /**
     * @var DynamoDbClient
     */
    public $client;

    public function __construct(DynamoDbClient $client, $debug = false)
    {
        $this->client = $client;
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
            return $this->client->{$method}($param->query);
        }

        throw new BadMethodCallException(sprintf(
            'Method %s::%s does not exist.', static::class, $method
        ));
    }
}
