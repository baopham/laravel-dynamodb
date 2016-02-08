<?php

namespace BaoPham\DynamoDb;

use Illuminate\Database\Eloquent\Collection;
use Illuminate\Database\Eloquent\Model;
use Illuminate\Support\Facades\App;
use Illuminate\Support\Facades\Log;

/**
 * Class DynamoDbModel.
 */
abstract class DynamoDbModel extends Model
{
    /**
     * Always set this to false since DynamoDb does not support incremental Id.
     *
     * @var bool
     */
    public $incrementing = false;

    /**
     * @var \BaoPham\DynamoDb\DynamoDbClientInterface
     */
    protected static $dynamoDb;

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

    /**
     * @var array
     */
    protected $where = [];

    /**
     * Indexes.
     * [
     *     'global_index_key' => 'global_index_name',
     *     'local_index_key' => 'local_index_name',
     * ].
     *
     * @var array
     */
    protected $dynamoDbIndexKeys = [];

    protected $compositeKey = [];

    public function __construct(array $attributes = [], DynamoDbClientService $dynamoDb = null)
    {
        $this->bootIfNotBooted();

        $this->syncOriginal();

        $this->fill($attributes);

        if (is_null(static::$dynamoDb)) {
            static::$dynamoDb = $dynamoDb;
        }

        $this->setupDynamoDb();
    }

    protected static function getInstance()
    {
        return new static();
    }

    protected function setupDynamoDb()
    {
        if (is_null(static::$dynamoDb)) {
            static::$dynamoDb = App::make('BaoPham\DynamoDb\DynamoDbClientInterface');
        }

        $this->client = static::$dynamoDb->getClient();
        $this->marshaler = static::$dynamoDb->getMarshaler();
        $this->attributeFilter = static::$dynamoDb->getAttributeFilter();
    }

    public function save(array $options = [])
    {
        if (!$this->getKey()) {
            $this->fireModelEvent('creating');
        }

        // $this->attributeFilter->filter($this->attributes);

        try {
            $this->client->putItem([
                'TableName' => $this->getTable(),
                'Item' => $this->marshalItem($this->attributes),
            ]);

            return true;
        } catch (Exception $e) {
            Log::info($e);

            return false;
        }
    }

    public function update(array $attributes = [], array $options = [])
    {
        return $this->fill($attributes)->save();
    }

    public static function create(array $attributes = [])
    {
        $model = static::getInstance();

        $model->fill($attributes)->save();

        return $model;
    }

    /**
     * Support composite keys here.
     */
    public function delete()
    {
        $key = $this->getModelKey($this->getKeyAsArray(), $this);

        $query = [
            'TableName' => $this->getTable(),
            'Key' => $key,
        ];

        $result = $this->client->deleteItem($query);
        $status = array_get($result->toArray(), '@metadata.statusCode');

        return $status == 200;
    }

    /**
     * Support composite keys here.
     */
    public static function find($id, array $columns = [])
    {
        $model = static::getInstance();

        $key = static::getModelKey($id, $model);

        $query = [
            'ConsistentRead' => true,
            'TableName' => $model->getTable(),
            'Key' => $key,
        ];

        if (!empty($columns)) {
            $query['AttributesToGet'] = $columns;
        }

        $item = $model->client->getItem($query);

        $item = array_get($item->toArray(), 'Item');

        if (empty($item)) {
            return;
        }

        $item = $model->unmarshalItem($item);

        $model->fill($item);

        if (is_array($id)) {
            if (isset($model->compositeKey) && !empty($model->compositeKey)) {
                foreach ($model->compositeKey as $var) {
                    $model->$var = $id[$var];
                }
            } else {
                $model->id = $id[$model->primaryKey];
            }
        } else {
            $model->id = $id;
        }

        return $model;
    }

    public static function all($columns = [], $limit = -1)
    {
        $model = static::getInstance();

        return $model->getAll($columns, $limit);
    }

    public static function first($columns = [])
    {
        $model = static::getInstance();
        $item = $model->getAll($columns, 1);

        return $item->first();
    }

    public static function where($column, $operator = null, $value = null, $boolean = 'and')
    {
        if ($boolean != 'and') {
            throw new NotSupportedException('Only support "and" in where clause');
        }

        $model = static::getInstance();

        // If the column is an array, we will assume it is an array of key-value pairs
        // and can add them each as a where clause. We will maintain the boolean we
        // received when the method was called and pass it into the nested where.
        if (is_array($column)) {
            foreach ($column as $key => $value) {
                return $model->where($key, '=', $value);
            }
        }

        // Here we will make some assumptions about the operator. If only 2 values are
        // passed to the method, we will assume that the operator is an equals sign
        // and keep going. Otherwise, we'll require the operator to be passed in.
        if (func_num_args() == 2) {
            list($value, $operator) = [$operator, '='];
        }

        // If the columns is actually a Closure instance, we will assume the developer
        // wants to begin a nested where statement which is wrapped in parenthesis.
        // We'll add that Closure to the query then return back out immediately.
        if ($column instanceof Closure) {
            throw new NotSupportedException('Closure in where clause is not supported');
        }

        // If the given operator is not found in the list of valid operators we will
        // assume that the developer is just short-cutting the '=' operators and
        // we will set the operators to '=' and set the values appropriately.
        if (!ComparisonOperator::isValidOperator($operator)) {
            list($value, $operator) = [$operator, '='];
        }

        // If the value is a Closure, it means the developer is performing an entire
        // sub-select within the query and we will need to compile the sub-select
        // within the where clause to get the appropriate query record results.
        if ($value instanceof Closure) {
            throw new NotSupportedException('Closure in where clause is not supported');
        }

        $attributeValueList = $model->marshalItem([
            'AttributeValueList' => $value,
        ]);

        $model->where[$column] = [
            'AttributeValueList' => [$attributeValueList['AttributeValueList']],
            'ComparisonOperator' => ComparisonOperator::getDynamoDbOperator($operator),
        ];

        return $model;
    }

    public function get($columns = [])
    {
        return $this->getAll($columns);
    }

    protected function getAll($columns = [], $limit = -1)
    {
        $query = [
            'TableName' => $this->getTable(),
        ];

        $op = 'Scan';

        if ($limit > -1) {
            $query['limit'] = $limit;
        }

        if (!empty($columns)) {
            $query['AttributesToGet'] = $columns;
        }

        // If the $where is not empty, we run getIterator.
        if (!empty($this->where)) {

            // Primary key or index key condition exists, then use Query instead of Scan.
            // However, Query only supports a few conditions.
            if ($key = $this->conditionsContainIndexKey()) {
                $condition = array_get($this->where, "$key.ComparisonOperator");

                if (ComparisonOperator::isValidQueryDynamoDbOperator($condition)) {
                    $op = 'Query';
                    $query['IndexName'] = $this->dynamoDbIndexKeys[$key];
                    $query['KeyConditions'] = $this->where;
                }
            }

            $query['ScanFilter'] = $this->where;
        }

        $iterator = $this->client->getIterator($op, $query);

        $results = [];
        foreach ($iterator as $item) {
            $item = $this->unmarshalItem($item);
            $model = new static($item, static::$dynamoDb);
            $model->setUnfillableAttributes($item);
            $results[] = $model;
        }

        return new Collection($results);
    }

    protected function conditionsContainIndexKey()
    {
        if (empty($this->where)) {
            return false;
        }

        foreach ($this->dynamoDbIndexKeys as $key => $name) {
            if (isset($this->where[$key])) {
                return $key;
            }
        }

        return false;
    }

    protected static function getDynamoDbPrimaryKey(DynamoDbModel $model, $id)
    {
        return static::getSpecificDynamoDbKey($model, $model->getKeyName(), $id);
    }

    protected static function getSpecificDynamoDbKey(DynamoDbModel $model, $keyName, $value)
    {
        $idKey = $model->marshalItem([
            $keyName => $value,
        ]);

        $key = [
            $keyName => $idKey[$keyName],
        ];

        return $key;
    }

    /**
     * Get the key for this model whether composite or simple.
     */
    protected static function getModelKey($id, DynamoDbModel $model)
    {
        if (is_array($id)) {
            $key = [];
            foreach ($id as $name => $value) {
                $specific_key = static::getSpecificDynamoDbKey($model, $name, $value);
                foreach ($specific_key as $key_name => $key_value) {
                    $key[$key_name] = $key_value;
                }
            }

            return $key;
        }

        return static::getDynamoDbPrimaryKey($model, $id);
    }

    protected function getKeyAsArray()
    {
        $result = [];

        if (!empty($this->compositeKey)) {
            foreach ($this->compositeKey as $key) {
                $result[$key] = $this->{$key};
            }
        } else {
            $result[$this->getKeyName()] = $this->getKey();
        }

        return $result;
    }

    protected function setUnfillableAttributes($attributes)
    {
        $keysToFill = array_diff(array_keys($attributes), $this->fillable);

        foreach ($keysToFill as $key) {
            $this->setAttribute($key, $attributes[$key]);
        }
    }

    public function marshalItem($item)
    {
        return $this->marshaler->marshalItem($item);
    }

    public function unmarshalItem($item)
    {
        return $this->marshaler->unmarshalItem($item);
    }
}
