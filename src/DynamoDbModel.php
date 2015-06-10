<?php

namespace BaoPham\DynamoDb;

use Illuminate\Database\Eloquent\Collection;
use Illuminate\Database\Eloquent\Model;
use Illuminate\Support\Facades\Log;
use Illuminate\Support\Facades\App;

/**
 * Class DynamoDbModel
 * @package BaoPham\DynamoDb
 */
abstract class DynamoDbModel extends Model
{

    /**
     * @var \BaoPham\DynamoDb\DynamoDbClientInterface
     */
    protected $dynamoDb;

    /**
     * @var \Aws\DynamoDb\DynamoDbClient
     */
    protected $client;

    /**
     * @var \Aws\DynamoDb\Marshaler
     */
    protected $marshaler;

    /**
     * @var \BaoPham\DynamoDb\EmptyAttributeFilter
     */
    protected $attributeFilter;

    public function __construct(array $attributes = [])
    {
        $this->bootIfNotBooted();

        $this->syncOriginal();

        $this->fill($attributes);

        $this->setupDynamoDb();
    }

    protected function setupDynamoDb()
    {
        $this->dynamoDb = App::make('BaoPham\DynamoDb\DynamoDbClientInterface');
        $this->client = $this->dynamoDb->getClient();
        $this->marshaler = $this->dynamoDb->getMarshaler();
        $this->attributeFilter = $this->dynamoDb->getAttributeFilter();
    }

    public function save(array $options = [])
    {
        if (!$this->getKey()) {
            $this->fireModelEvent('creating');
        }

        $this->attributeFilter->filter($this->attributes);

        try {
             $this->client->putItem([
                 'TableName' => $this->getTable(),
                 'Item' => $this->marshaler->marshalItem($this->attributes),
             ]);

            return true;
        } catch (Exception $e) {
            Log::info($e);
            return false;
        }
    }

    public function update(array $attributes = [])
    {
        return $this->fill($attributes)->save();
    }

    public static function create(array $attributes = [])
    {
        $model = new static;

        $model->fill($attributes)->save();

        return $model;
    }

    public function delete()
    {
        // TODO
    }

    public static function find($id, array $columns = [])
    {
        $model = new static;

        $keyName = $model->getKeyName();

        $idKey = $model->marshaler->marshalItem([
            $keyName => $id
        ]);

        $query = [
            'ConsistentRead' => true,
            'TableName' => $model->getTable(),
            'Key'       => [
                $keyName => $idKey[$keyName]
            ]
        ];

        if (!empty($columns)) {
            $query['AttributesToGet'] = $columns;
        }

        $item = $model->client->getItem($query);

        $item = $item->toArray();

        $item = array_get($item, 'Item');

        $item = $model->marshaler->unmarshalItem($item);

        return (object) $item;
    }

    public static function all($columns = [], $limit = -1)
    {
        $model = new static;

        $query = [];

        if (!empty($columns)) {
            $query['AttributesToGet'] = $columns;
        }

        if ($limit > -1) {
            $query['Limit'] = $limit;
        }

        $query['TableName'] = $model->getTable();

        $items = $model->client->scan($query);

        $items = array_get($items->toArray(), 'Items');

        $results = [];

        foreach ($items as $item) {
            $results[] = (object) $model->marshaler->unmarshalItem($item);
        }

        return new Collection($results);
    }

    public function first($columns = [])
    {
        $item = static::all($columns, 1);
        return $item->first();
    }

    public static function where(array $attributes)
    {
        // TODO
    }
}
