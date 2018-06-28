<?php

namespace BaoPham\DynamoDb\Facades;

use Illuminate\Support\Facades\Facade;

/**
 * @method static \Aws\DynamoDb\DynamoDbClient client()
 * @method static \BaoPham\DynamoDb\DynamoDb\QueryBuilder table($name)
 * @method static \BaoPham\DynamoDb\DynamoDb\QueryBuilder newQuery()
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
