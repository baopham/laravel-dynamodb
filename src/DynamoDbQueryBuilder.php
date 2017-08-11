<?php

namespace BaoPham\DynamoDb;

use Aws\DynamoDb\DynamoDbClient;
use Exception;
use Illuminate\Database\Eloquent\Collection;
use Illuminate\Support\Facades\Log;

class DynamoDbQueryBuilder
{
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
     * @var int
     */
    protected $limit = -1;

    /**
     * The methods that should be returned from query builder.
     *
     * @var array
     */
    protected $passthru = [
        'insert', 'insertGetId', 'getBindings', 'toSql',
        'exists', 'count', 'min', 'max', 'avg', 'sum', 'getConnection',
    ];

    /**
     * Applied global scopes.
     *
     * @var array
     */
    protected $scopes = [];

    /**
     * Removed global scopes.
     *
     * @var array
     */
    protected $removedScopes = [];

    /**
     * When not using the iterator, you can store the lastEvaluatedKey to
     * paginate through the results. The getAll method will take this into account
     * when used with $use_iterator = false.
     *
     * @var mixed
     */
    protected $lastEvaluatedKey;

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

        if (strtolower($operator) === 'between') {
            $valueList = head($valueList)['L'];
        }

        $this->where[$column] = [
            'AttributeValueList' => $valueList,
            'ComparisonOperator' => ComparisonOperator::getDynamoDbOperator($operator),
        ];

        return $this;
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

    public function first($columns = [])
    {
        $item = $this->getAll($columns, 1);

        return $item->first();
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

        $this->applyScopes();

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

    /**
     * Register a new global scope.
     *
     * @param  string  $identifier
     * @param  \Illuminate\Database\Eloquent\Scope|\Closure  $scope
     * @return $this
     */
    public function withGlobalScope($identifier, $scope)
    {
        $this->scopes[$identifier] = $scope;

        if (method_exists($scope, 'extend')) {
            $scope->extend($this);
        }

        return $this;
    }

    /**
     * Remove a registered global scope.
     *
     * @param  \Illuminate\Database\Eloquent\Scope|string  $scope
     * @return $this
     */
    public function withoutGlobalScope($scope)
    {
        if (! is_string($scope)) {
            $scope = get_class($scope);
        }

        unset($this->scopes[$scope]);

        $this->removedScopes[] = $scope;

        return $this;
    }

    /**
     * Remove all or passed registered global scopes.
     *
     * @param  array|null  $scopes
     * @return $this
     */
    public function withoutGlobalScopes(array $scopes = null)
    {
        if (is_array($scopes)) {
            foreach ($scopes as $scope) {
                $this->withoutGlobalScope($scope);
            }
        } else {
            $this->scopes = [];
        }

        return $this;
    }

    /**
     * Get an array of global scopes that were removed from the query.
     *
     * @return array
     */
    public function removedScopes()
    {
        return $this->removedScopes;
    }

    /**
     * Apply the scopes to the Eloquent builder instance and return it.
     *
     * @return \Illuminate\Database\Eloquent\Builder|static
     */
    public function applyScopes()
    {
        if (! $this->scopes) {
            return $this;
        }

        $builder = clone $this;

        foreach ($this->scopes as $identifier => $scope) {
            if (! isset($builder->scopes[$identifier])) {
                continue;
            }

            $builder->callScope(function (DynamoDbQueryBuilder $builder) use ($scope) {
                // If the scope is a Closure we will just go ahead and call the scope with the
                // builder instance. The "callScope" method will properly group the clauses
                // that are added to this query so "where" clauses maintain proper logic.
                if ($scope instanceof Closure) {
                    $scope($builder);
                }

                // If the scope is a scope object, we will call the apply method on this scope
                // passing in the builder and the model instance. After we run all of these
                // scopes we will return back the builder instance to the outside caller.
                if ($scope instanceof Scope) {
                    $scope->apply($builder, $this->getModel());
                }
            });
        }

        return $builder;
    }

    /**
     * Apply the given scope on the current builder instance.
     *
     * @param  callable  $scope
     * @param  array  $parameters
     * @return mixed
     */
    protected function callScope(callable $scope, $parameters = [])
    {
        array_unshift($parameters, $this);

        // $query = $this->getQuery();

        // // We will keep track of how many wheres are on the query before running the
        // // scope so that we can properly group the added scope constraints in the
        // // query as their own isolated nested where statement and avoid issues.
        // $originalWhereCount = is_null($query->wheres)
        //             ? 0 : count($query->wheres);

        // var_dump($scope);

        $result = $scope(...array_values($parameters)) ?: $this;

        // if (count((array) $query->wheres) > $originalWhereCount) {
        //     $this->addNewWheresWithinGroup($query, $originalWhereCount);
        // }

        return $result;
    }
}
