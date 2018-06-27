<?php

namespace BaoPham\DynamoDb\Facades;

use Aws\DynamoDb\DynamoDbClient;
use BaoPham\DynamoDb\DynamoDb\DynamoDbManager;
use BaoPham\DynamoDb\DynamoDb\QueryBuilder;
use Illuminate\Support\Facades\Facade;

/**
 * @method static DynamoDbClient client()
 * @method static QueryBuilder table($name)
 * @method static QueryBuilder newQuery()
 * @method static array marshalItem($item)
 * @method static array marshalValue($value)
 * @method static mixed unmarshalItem($value)
 * @method static mixed unmarshalValue($value)
 *
 * @see DynamoDbManager
 */
class DynamoDb extends Facade
{
    /**
     * Get the registered name of the component.
     *
     * @return string
     */
    protected static function getFacadeAccessor()
    {
        return 'dynamodb';
    }
}
