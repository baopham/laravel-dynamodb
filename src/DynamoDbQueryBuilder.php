<?php

namespace BaoPham\DynamoDb;

use Aws\DynamoDb\DynamoDbClient;
use Closure;
use Exception;
use Illuminate\Contracts\Support\Arrayable;
use Illuminate\Database\Eloquent\Collection;
use \Illuminate\Database\Eloquent\ModelNotFoundException;
use Illuminate\Support\Facades\Log;

class DynamoDbQueryBuilder
{
    /**
     * The maximum number of records to return.
     *
     * @var int
     */
    public $limit;

    /**
     * @var array
     */
    protected $where = [];

    /**
     * @var DynamoDbModel
     */
    protected $model;

    /**
     * @var DynamoDbClient
     */
    protected $client;

    /**
     * When not using the iterator, you can store the lastEvaluatedKey to
     * paginate through the results. The getAll method will take this into account
     * when used with $use_iterator = false.
     *
     * @var mixed
     */
    protected $lastEvaluatedKey;

    /**
     * Alias to set the "limit" value of the query.
     *
     * @param  int  $value
     * @return DynamoDbQueryBuilder\static
     */
    public function take($value)
    {
        return $this->limit($value);
    }

    /**
     * Set the "limit" value of the query.
     *
     * @param  int  $value
     * @return $this
     */
    public function limit($value)
    {
        $this->limit = $value;

        return $this;
    }

    public function where($column, $operator = null, $value = null, $boolean = 'and')
    {
        if ($boolean != 'and') {
            throw new NotSupportedException('Only support "and" in where clause');
        }

        $model = $this->getModel();

        // If the column is an array, we will assume it is an array of key-value pairs
        // and can add them each as a where clause. We will maintain the boolean we
        // received when the method was called and pass it into the nested where.
        if (is_array($column)) {
            foreach ($column as $key => $value) {
                return $this->where($key, '=', $value);
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

        $valueList = [$attributeValueList['AttributeValueList']];

        if (
            ComparisonOperator::is($operator, ComparisonOperator::BETWEEN) ||
            ComparisonOperator::is($operator, ComparisonOperator::IN)
        ) {
            $valueList = head($valueList)['L'];
        }

        $this->where[$column] = [
            'AttributeValueList' => $valueList,
            'ComparisonOperator' => ComparisonOperator::getDynamoDbOperator($operator),
        ];

        return $this;
    }

    /**
     * Add a "where in" clause to the query.
     *
     * @param  string  $column
     * @param  mixed   $values
     * @param  string  $boolean
     * @param  bool    $not
     * @return $this
     */
    public function whereIn($column, $values, $boolean = 'and', $not = false)
    {
        if ($not) {
            throw new NotSupportedException('"not in" is not a valid DynamoDB comparison operator');
        }

        // If the value is a query builder instance, not supported
        if ($values instanceof static) {
            throw new NotSupportedException('Value is a query builder instance');
        }

        // If the value of the where in clause is actually a Closure, not supported
        if ($values instanceof Closure) {
            throw new NotSupportedException('Value is a Closure');
        }

        // Next, if the value is Arrayable we need to cast it to its raw array form
        if ($values instanceof Arrayable) {
            $values = $values->toArray();
        }

        return $this->where($column, ComparisonOperator::IN, $values, $boolean);
    }

    /**
     * Add a "where null" clause to the query.
     *
     * @param  string  $column
     * @param  string  $boolean
     * @param  bool    $not
     * @return $this
     */
    public function whereNull($column, $boolean = 'and', $not = false)
    {
        if ($boolean != 'and') {
            throw new NotSupportedException('Only support "and" in whereNull clause');
        }

        $operator = $not ? 'not_null' : 'null';

        $this->where[$column] = [
            'ComparisonOperator' => ComparisonOperator::getDynamoDbOperator($operator),
        ];

        return $this;
    }

    /**
     * Add a "where not null" clause to the query.
     *
     * @param  string  $column
     * @param  string  $boolean
     * @return $this
     */
    public function whereNotNull($column, $boolean = 'and')
    {
        return $this->whereNull($column, $boolean, true);
    }

    /**
     * Implements the Query Chunk method
     *
     * @param int $chunk_size
     * @param callable $callback
     */
    public function chunk($chunk_size, callable $callback)
    {
        while (true) {
            $results = $this->getAll([], $chunk_size, false);

            call_user_func($callback, $results);

            if (empty($this->lastEvaluatedKey)) {
                break;
            }
        }
    }

    public function find($id, array $columns = [])
    {
        if ($this->isMultipleIds($id)) {
            return $this->findMany($id, $columns);
        }

        $model = $this->model;

        $model->setId($id);

        $key = $this->getDynamoDbKey();

        $query = [
            'ConsistentRead' => true,
            'TableName' => $model->getTable(),
            'Key' => $key,
        ];

        if (!empty($columns)) {
            $query['AttributesToGet'] = $columns;
        }

        $item = $this->client->getItem($query);

        $item = array_get($item->toArray(), 'Item');

        if (empty($item)) {
            return;
        }

        $item = $model->unmarshalItem($item);

        $model->fill($item);

        $model->setUnfillableAttributes($item);

        $model->syncOriginal();

        $model->exists = true;

        return $model;
    }

    public function findMany($ids, array $columns = [])
    {
        throw new NotSupportedException('Finding by multiple ids is not supported');
    }

    public function findOrFail($id, $columns = [])
    {
        $result = $this->find($id, $columns);

        if ($this->isMultipleIds($id)) {
            if (count($result) == count(array_unique($id))) {
                return $result;
            }
        } elseif (! is_null($result)) {
            return $result;
        }

        throw (new ModelNotFoundException)->setModel(
            get_class($this->model), $id
        );
    }

    public function first($columns = [])
    {
        $item = $this->getAll($columns, 1);

        return $item->first();
    }

    public function firstOrFail($columns = [])
    {
        if (! is_null($model = $this->first($columns))) {
            return $model;
        }

        throw (new ModelNotFoundException)->setModel(get_class($this->model));
    }

    public function get($columns = [])
    {
        return $this->getAll($columns);
    }

    public function delete()
    {
        $key = $this->getDynamoDbKey();

        $query = [
            'TableName' => $this->model->getTable(),
            'Key' => $key,
        ];

        $result = $this->client->deleteItem($query);
        $status = array_get($result->toArray(), '@metadata.statusCode');

        return $status == 200;
    }

    public function save()
    {
        try {
            $this->client->putItem([
                'TableName' => $this->model->getTable(),
                'Item' => $this->model->marshalItem($this->model->getAttributes()),
            ]);

            return true;
        } catch (Exception $e) {
            Log::info($e);

            return false;
        }
    }

    public function all($columns = [])
    {
        return $this->getAll($columns);
    }

    public function count()
    {
        return $this->getAll([$this->model->getKeyName()])->count();
    }

    protected function getAll($columns = [], $limit = -1, $use_iterator = true)
    {
        if ($limit === -1 && isset($this->limit)) {
            $limit = $this->limit;
        }

        if ($conditionValue = $this->conditionsContainKey()) {
            if ($this->conditionsAreExactSearch()) {
                $item = $this->find($conditionValue, $columns);

                return new Collection([$item]);
            }
        }

        $query = [
            'TableName' => $this->model->getTable(),
        ];

        $op = 'Scan';

        if ($limit > -1) {
            $query['Limit'] = $limit;
        }

        if (!empty($columns)) {
            $query['AttributesToGet'] = $columns;
        }

        if (!empty($this->lastEvaluatedKey)) {
            $query['ExclusiveStartKey'] = $this->lastEvaluatedKey;
        }

        // If the $where is not empty, we run getIterator.
        if (!empty($this->where)) {
            // Index key condition exists, then use Query instead of Scan.
            // However, Query only supports a few conditions.
            if ($index = $this->conditionsContainIndexKey()) {
                $keysInfo = $index['keysInfo'];
                $isCompositeKey = isset($keysInfo['range']);
                $hashKeyOperator = array_get($this->where, $keysInfo['hash'] . '.ComparisonOperator');
                $isValidQueryDynamoDbOperator = ComparisonOperator::isValidQueryDynamoDbOperator($hashKeyOperator);

                if ($isValidQueryDynamoDbOperator && $isCompositeKey) {
                    $rangeKeyOperator = array_get($this->where, $keysInfo['range'] . '.ComparisonOperator');
                    $isValidQueryDynamoDbOperator = ComparisonOperator::isValidQueryDynamoDbOperator($rangeKeyOperator, true);
                }

                if ($isValidQueryDynamoDbOperator) {
                    $op = 'Query';
                    $query['IndexName'] = $index['name'];
                    $query['KeyConditions'][$keysInfo['hash']] = array_get($this->where, $keysInfo['hash']);

                    if ($isCompositeKey) {
                        $query['KeyConditions'][$keysInfo['range']] = array_get($this->where, $keysInfo['range']);
                    }

                    $nonKeyConditions = array_except($this->where, array_values($keysInfo));

                    if (!empty($nonKeyConditions)) {
                        $query['QueryFilter'] = $nonKeyConditions;
                    }
                }
            } else if ($this->conditionsContainKey()) {
                $op = 'Query';
                $query['KeyConditions'] = $this->where;
            }

            if ($op === 'Scan') {
                $query['ScanFilter'] = $this->where;
            }
        }

        if ($use_iterator) {
            $iterator = $this->client->getIterator($op, $query);

            if (isset($query['Limit'])) {
                $iterator = new \LimitIterator($iterator, 0, $query['Limit']);
            }
        } else {
            if ($op === 'Scan') {
                $res = $this->client->scan($query);
            } else {
                $res = $this->client->query($query);
            }

            $this->lastEvaluatedKey = array_get($res, 'LastEvaluatedKey');
            $iterator = $res['Items'];
        }

        $results = [];

        foreach ($iterator as $item) {
            $item = $this->model->unmarshalItem($item);
            $model = $this->model->newInstance($item, true);
            $model->setUnfillableAttributes($item);
            $model->syncOriginal();
            $results[] = $model;
        }

        return new Collection($results);
    }

    protected function conditionsAreExactSearch()
    {
        if (empty($this->where)) {
            return false;
        }

        foreach ($this->where as $condition) {
            if (array_get($condition, 'ComparisonOperator') !== ComparisonOperator::EQ) {
                return false;
            }
        }

        return true;
    }

    /**
     * Check if conditions "where" contain primary key or composite key.
     * For composite key, it will return false if the conditions don't have all composite key.
     *
     * For example:
     *   Consider a composite key condition:
     *     $model->where('partition_key', 'foo')->where('sort_key', 'bar')
     *   We return ['partition_key' => 'foo', 'sort_key' => 'bar'] since the conditions
     *   contain all the composite key.
     *
     * @return array|bool the condition value
     */
    protected function conditionsContainKey()
    {
        if (empty($this->where)) {
            return false;
        }

        $conditionKeys = array_keys($this->where);

        $model = $this->model;

        $keys = $model->hasCompositeKey() ? $model->getCompositeKey() : [$model->getKeyName()];

        $conditionsContainKey = count(array_intersect($conditionKeys, $keys)) === count($keys);

        if (!$conditionsContainKey) {
            return false;
        }

        $conditionValue = [];

        foreach ($keys as $key) {
            $condition = $this->where[$key];

            $value = $model->unmarshalItem(array_get($condition, 'AttributeValueList'))[0];

            $conditionValue[$key] = $value;
        }

        return $conditionValue;
    }

    /**
     * Check if conditions "where" contain index key
     * For composite index key, it will return false if the conditions don't have all composite key.
     *
     * @return array|bool false or array ['name' => 'index_name', 'keysInfo' => ['hash' => 'hash_key', 'range' => 'range_key']]
     */
    protected function conditionsContainIndexKey()
    {
        if (empty($this->where)) {
            return false;
        }

        foreach ($this->model->getDynamoDbIndexKeys() as $name => $keysInfo) {
            $conditionKeys = array_keys($this->where);
            $keys = array_values($keysInfo);
            if (count(array_intersect($conditionKeys, $keys)) === count($keys)) {
                return [
                    'name' => $name,
                    'keysInfo' => $keysInfo
                ];
            }
        }

        return false;
    }

    protected function getDynamoDbKey()
    {
        if (!$this->model->hasCompositeKey()) {
            return $this->getDynamoDbPrimaryKey();
        }

        $keys = [];

        foreach ($this->model->getCompositeKey() as $key) {
            $keys = array_merge($keys, $this->getSpecificDynamoDbKey($key, $this->model->getAttribute($key)));
        }

        return $keys;
    }

    protected function getDynamoDbPrimaryKey()
    {
        return $this->getSpecificDynamoDbKey($this->model->getKeyName(), $this->model->getKey());
    }

    protected function getSpecificDynamoDbKey($keyName, $value)
    {
        $idKey = $this->model->marshalItem([
            $keyName => $value,
        ]);

        $key = [
            $keyName => $idKey[$keyName],
        ];

        return $key;
    }

    protected function isMultipleIds($id)
    {
        $hasCompositeKey = $this->model->hasCompositeKey();

        if (!$hasCompositeKey && isset($id[$this->model->getKeyName()])) {
            return false;
        }

        if ($hasCompositeKey) {
            $compositeKey = $this->model->getCompositeKey();
            if (isset($id[$compositeKey[0]]) && isset($id[$compositeKey[1]])) {
                return false;
            }
        }

        return $hasCompositeKey ? is_array(array_first($id)) : is_array($id);
    }

    /**
     * @return DynamoDbModel
     */
    public function getModel()
    {
        return $this->model;
    }

    /**
     * @param DynamoDbModel $model
     */
    public function setModel($model)
    {
        $this->model = $model;
    }

    /**
     * @return DynamoDbClient
     */
    public function getClient()
    {
        return $this->client;
    }

    /**
     * @param DynamoDbClient $client
     */
    public function setClient($client)
    {
        $this->client = $client;
    }
}
