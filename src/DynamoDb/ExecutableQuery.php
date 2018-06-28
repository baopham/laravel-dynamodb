<?php

namespace BaoPham\DynamoDb\DynamoDb;

use Aws\DynamoDb\DynamoDbClient;

/**
 * Class ExecutableQuery
 *
 * @package BaoPham\DynamoDb\DynamoDb
 *
 * @method \Aws\Result batchGetItem()
 * @method \GuzzleHttp\Promise\Promise batchGetItemAsync()
 * @method \Aws\Result batchWriteItem()
 * @method \GuzzleHttp\Promise\Promise batchWriteItemAsync()
 * @method \Aws\Result createTable()
 * @method \GuzzleHttp\Promise\Promise createTableAsync()
 * @method \Aws\Result deleteItem()
 * @method \GuzzleHttp\Promise\Promise deleteItemAsync()
 * @method \Aws\Result deleteTable()
 * @method \GuzzleHttp\Promise\Promise deleteTableAsync()
 * @method \Aws\Result describeTable()
 * @method \GuzzleHttp\Promise\Promise describeTableAsync()
 * @method \Aws\Result getItem()
 * @method \GuzzleHttp\Promise\Promise getItemAsync()
 * @method \Aws\Result listTables()
 * @method \GuzzleHttp\Promise\Promise listTablesAsync()
 * @method \Aws\Result putItem()
 * @method \GuzzleHttp\Promise\Promise putItemAsync()
 * @method \Aws\Result query()
 * @method \GuzzleHttp\Promise\Promise queryAsync()
 * @method \Aws\Result scan()
 * @method \GuzzleHttp\Promise\Promise scanAsync()
 * @method \Aws\Result updateItem()
 * @method \GuzzleHttp\Promise\Promise updateItemAsync()
 * @method \Aws\Result updateTable()
 * @method \GuzzleHttp\Promise\Promise updateTableAsync()
 * @method \Aws\Result createBackup() (supported in versions 2012-08-10)
 * @method \GuzzleHttp\Promise\Promise createBackupAsync() (supported in versions 2012-08-10)
 * @method \Aws\Result createGlobalTable() (supported in versions 2012-08-10)
 * @method \GuzzleHttp\Promise\Promise createGlobalTableAsync() (supported in versions 2012-08-10)
 * @method \Aws\Result deleteBackup() (supported in versions 2012-08-10)
 * @method \GuzzleHttp\Promise\Promise deleteBackupAsync() (supported in versions 2012-08-10)
 * @method \Aws\Result describeBackup() (supported in versions 2012-08-10)
 * @method \GuzzleHttp\Promise\Promise describeBackupAsync() (supported in versions 2012-08-10)
 * @method \Aws\Result describeContinuousBackups() (supported in versions 2012-08-10)
 * @method \GuzzleHttp\Promise\Promise describeContinuousBackupsAsync() (supported in versions 2012-08-10)
 * @method \Aws\Result describeGlobalTable() (supported in versions 2012-08-10)
 * @method \GuzzleHttp\Promise\Promise describeGlobalTableAsync() (supported in versions 2012-08-10)
 * @method \Aws\Result describeLimits() (supported in versions 2012-08-10)
 * @method \GuzzleHttp\Promise\Promise describeLimitsAsync() (supported in versions 2012-08-10)
 * @method \Aws\Result describeTimeToLive() (supported in versions 2012-08-10)
 * @method \GuzzleHttp\Promise\Promise describeTimeToLiveAsync() (supported in versions 2012-08-10)
 * @method \Aws\Result listBackups() (supported in versions 2012-08-10)
 * @method \GuzzleHttp\Promise\Promise listBackupsAsync() (supported in versions 2012-08-10)
 * @method \Aws\Result listGlobalTables() (supported in versions 2012-08-10)
 * @method \GuzzleHttp\Promise\Promise listGlobalTablesAsync() (supported in versions 2012-08-10)
 * @method \Aws\Result listTagsOfResource() (supported in versions 2012-08-10)
 * @method \GuzzleHttp\Promise\Promise listTagsOfResourceAsync() (supported in versions 2012-08-10)
 * @method \Aws\Result restoreTableFromBackup() (supported in versions 2012-08-10)
 * @method \GuzzleHttp\Promise\Promise restoreTableFromBackupAsync() (supported in versions 2012-08-10)
 * @method \Aws\Result tagResource() (supported in versions 2012-08-10)
 * @method \GuzzleHttp\Promise\Promise tagResourceAsync() (supported in versions 2012-08-10)
 * @method \Aws\Result untagResource() (supported in versions 2012-08-10)
 * @method \GuzzleHttp\Promise\Promise untagResourceAsync() (supported in versions 2012-08-10)
 * @method \Aws\Result updateGlobalTable() (supported in versions 2012-08-10)
 * @method \GuzzleHttp\Promise\Promise updateGlobalTableAsync() (supported in versions 2012-08-10)
 * @method \Aws\Result updateTimeToLive() (supported in versions 2012-08-10)
 * @method \GuzzleHttp\Promise\Promise updateTimeToLiveAsync() (supported in versions 2012-08-10)
 */
class ExecutableQuery
{
    /**
     * @var DynamoDbClient
     */
    private $client;

    /**
     * @var array
     */
    public $query;

    public function __construct(DynamoDbClient $client, array $query)
    {
        $this->client = $client;
        $this->query = $query;
    }

    /**
     * @param  string $method
     * @param  array  $parameters
     * @return mixed
     */
    public function __call($method, $parameters)
    {
        return $this->client->{$method}($this->query);
    }
}
