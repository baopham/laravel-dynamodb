<?php

namespace BaoPham\DynamoDb;

use BaoPham\DynamoDb\Concerns\HasParsers;
use Closure;
use Exception;
use Aws\DynamoDb\DynamoDbClient;
use Illuminate\Contracts\Support\Arrayable;
use \Illuminate\Database\Eloquent\ModelNotFoundException;
use Illuminate\Support\Facades\Log;

class DynamoDbQueryBuilder
{
    use HasParsers;

    /**
     * The maximum number of records to return.
     *
     * @var int
     */
    public $limit;

    /**
     * @var array
     */
    public $wheres = [];

    /**
     * @var DynamoDbModel
     */
    protected $model;

    /**
     * @var DynamoDbClient
     */
    protected $client;

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

    public function __construct(DynamoDbModel $model)
    {
        $this->model = $model;
        $this->client = $model->getClient();
        $this->setupExpressions();
    }

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
            return $this->whereNested($column, $boolean);
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

        $this->wheres[] = [
            'column' => $column,
            'type' => ComparisonOperator::getDynamoDbOperator($operator),
            'value' => $value,
            'boolean' => $boolean,
        ];

        return $this;
    }

    /**
     * Add a nested where statement to the query.
     *
     * @param  \Closure $callback
     * @param  string   $boolean
     * @return DynamoDbQueryBuilder|static
     */
    public function whereNested(Closure $callback, $boolean = 'and')
    {
        call_user_func($callback, $query = $this->forNestedWhere());

        return $this->addNestedWhereQuery($query, $boolean);
    }

    /**
     * Create a new query instance for nested where condition.
     *
     * @return DynamoDbQueryBuilder
     */
    public function forNestedWhere()
    {
        return $this->newQuery();
    }

    /**
     * Add another query builder as a nested where to the query builder.
     *
     * @param  DynamoDbQueryBuilder|static $query
     * @param  string  $boolean
     * @return $this
     */
    public function addNestedWhereQuery($query, $boolean = 'and')
    {
        if (count($query->wheres)) {
            $type = 'Nested';
            $column = null;
            $value = $query->wheres;
            $this->wheres[] = compact('column', 'type', 'value', 'boolean');
        }

        return $this;
    }

    /**
     * Add an "or where" clause to the query.
     *
     * @param  string  $column
     * @param  string  $operator
     * @param  mixed   $value
     * @return $this
     */
    public function orWhere($column, $operator = null, $value = null)
    {
        return $this->where($column, $operator, $value, 'or');
    }

    /**
     * Add a "where in" clause to the query.
     *
     * @param  string  $column
     * @param  mixed   $values
     * @param  string  $boolean
     * @param  bool    $not
     * @return $this
     * @throws NotSupportedException
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
     * Add an "or where in" clause to the query.
     *
     * @param  string  $column
     * @param  mixed   $values
     * @return $this
     */
    public function orWhereIn($column, $values)
    {
        return $this->whereIn($column, $values, 'or');
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
        $type = $not ? ComparisonOperator::NOT_NULL : ComparisonOperator::NULL;

        $this->wheres[] = compact('column', 'type', 'boolean');

        return $this;
    }

    /**
     * Add an "or where null" clause to the query.
     *
     * @param  string  $column
     * @return $this
     */
    public function orWhereNull($column)
    {
        return $this->whereNull($column, 'or');
    }

    /**
     * Add an "or where not null" clause to the query.
     *
     * @param  string  $column
     * @return $this
     */
    public function orWhereNotNull($column)
    {
        return $this->whereNotNull($column, 'or');
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
     * Get a new instance of the query builder.
     *
     * @return DynamoDbQueryBuilder
     */
    public function newQuery()
    {
        return new static($this->getModel());
    }

    /**
     * Implements the Query Chunk method
     *
     * @param int $chunk_size
     * @param callable $callback
     */
    public function chunk($chunk_size, callable $callback)
    {
        $this->applyScopes();
        while (true) {
            $results = $this->getAll([], $chunk_size, false, false);

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

        $this->model->setId($id);

        $key = $this->getDynamoDbKey();

        $query = [
            'ConsistentRead' => true,
            'TableName' => $this->model->getTable(),
            'Key' => $key,
        ];

        if (!empty($columns)) {
            $query['ProjectionExpression'] = $this->projectionExpression->parse($columns);
        }

        $item = $this->client->getItem($query);

        $item = array_get($item->toArray(), 'Item');

        if (empty($item)) {
            return;
        }

        $item = $this->model->unmarshalItem($item);

        $model = $this->model->newInstance([], true);

        $model->setRawAttributes($item, true);

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
            get_class($this->model),
            $id
        );
    }

    public function first($columns = [])
    {
        $items = $this->getAll($columns, 1);

        return $items->first();
    }

    public function firstOrFail($columns = [])
    {
        if (! is_null($model = $this->first($columns))) {
            return $model;
        }

        throw (new ModelNotFoundException)->setModel(get_class($this->model));
    }

    /**
     * Remove attributes from an existing item
     *
     * @param array ...$attributes
     * @return bool
     * @throws InvalidQuery
     */
    public function removeAttribute(...$attributes)
    {
        $key = $this->getDynamoDbKey();

        if (empty($key)) {
            $conditionValue = $this->conditionsContainKey();

            if (!$conditionValue || !$this->conditionsAreExactSearch()) {
                throw new InvalidQuery('Need to provide the key in your query');
            }

            $this->model->setId($conditionValue);
            $key = $this->getDynamoDbKey();
        }

        $this->resetExpressions();

        try {
            $query = [
                'TableName' => $this->model->getTable(),
                'Key' => $key,
                'UpdateExpression' => $this->updateExpression->remove($attributes),
                'ExpressionAttributeNames' => $this->expressionAttributeNames->all(),
            ];
            $this->client->updateItem($this->cleanUpQuery($query));

            return true;
        } catch (Exception $e) {
            Log::error($e);

            return false;
        }
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
            Log::error($e);

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

    protected function getAll($columns = [], $limit = -1, $use_iterator = true, $apply_scopes = true)
    {
        if($apply_scopes) $this->applyScopes();

        if ($limit === -1 && isset($this->limit)) {
            $limit = $this->limit;
        }
        if ($conditionValue = $this->conditionsContainKey()) {
            if ($this->conditionsAreExactSearch()) {
                $item = $this->find($conditionValue, $columns);

                return $this->getModel()->newCollection([$item]);
            }
        }

        $query = [
            'TableName' => $this->model->getTable(),
        ];

        if ($limit > -1) {
            $query['Limit'] = $limit;
        }

        if (!empty($columns)) {
            $query['ProjectionExpression'] = $this->projectionExpression->parse($columns);
        }

        if (!empty($this->lastEvaluatedKey)) {
            $query['ExclusiveStartKey'] = $this->lastEvaluatedKey;
        }

        $queryInfo = $this->buildExpressionQuery();
        $op = $queryInfo['op'];
        $query = array_merge($query, $queryInfo['query']);
        $query = $this->cleanUpQuery($query);

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
            $model = $this->model->newInstance([], true);
            $model->setRawAttributes($item, true);
            $results[] = $model;
        }

        return $this->getModel()->newCollection($results);
    }

    protected function buildExpressionQuery()
    {
        $this->resetExpressions();

        $op = 'Scan';
        $query = [];

        if (empty($this->wheres)) {
            return compact('op', 'query');
        }

        // Index key condition exists, then use Query instead of Scan.
        // However, Query only supports a few conditions.
        if ($index = $this->conditionsContainIndexKey()) {
            $keysInfo = $index['keysInfo'];

            $isCompositeKey = isset($keysInfo['range']);

            $hashKeyCondition = array_first($this->wheres, function ($condition) use ($keysInfo) {
                return $condition['column'] === $keysInfo['hash'];
            });

            $isValidQueryOperator = ComparisonOperator::isValidQueryDynamoDbOperator($hashKeyCondition['type']);

            if ($isValidQueryOperator && $isCompositeKey) {
                $rangeKeyCondition = array_first($this->wheres, function ($condition) use ($keysInfo) {
                    return $condition['column'] === $keysInfo['range'];
                });

                $isValidQueryOperator = ComparisonOperator::isValidQueryDynamoDbOperator(
                    $rangeKeyCondition['type'],
                    true
                );
            }

            if ($isValidQueryOperator) {
                $op = 'Query';

                $indexes = array_values($keysInfo);

                $keyConditions = array_filter($this->wheres, function ($condition) use ($indexes) {
                    return in_array($condition['column'], $indexes);
                });

                $nonKeyConditions = array_filter($this->wheres, function ($condition) use ($indexes) {
                    return !in_array($condition['column'], $indexes);
                });

                $query['IndexName'] = $index['name'];

                $query['KeyConditionExpression'] = $this->keyConditionExpression->parse($keyConditions);

                $query['FilterExpression'] = $this->filterExpression->parse($nonKeyConditions);
            }
        } elseif ($this->conditionsContainKey()) {
            $op = 'Query';

            $query['KeyConditionExpression'] = $this->keyConditionExpression->parse($this->wheres);
        }

        if ($op === 'Scan') {
            $query['FilterExpression'] = $this->filterExpression->parse($this->wheres);
        }

        $query['ExpressionAttributeNames'] = $this->expressionAttributeNames->all();

        $query['ExpressionAttributeValues'] = $this->expressionAttributeValues->all();

        return compact('op', 'query');
    }

    protected function conditionsAreExactSearch()
    {
        if (empty($this->wheres)) {
            return false;
        }

        foreach ($this->wheres as $condition) {
            if (array_get($condition, 'type') !== ComparisonOperator::EQ) {
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
        if (empty($this->wheres)) {
            return false;
        }

        $conditionKeys = array_pluck($this->wheres, 'column');

        $model = $this->model;

        $keys = $model->hasCompositeKey() ? $model->getCompositeKey() : [$model->getKeyName()];

        $conditionsContainKey = count(array_intersect($conditionKeys, $keys)) === count($keys);

        if (!$conditionsContainKey) {
            return false;
        }

        $conditionValue = [];

        foreach ($this->wheres as $condition) {
            $column = array_get($condition, 'column');
            if (in_array($column, $keys)) {
                $conditionValue[$column] = array_get($condition, 'value');
            }
        }

        return $conditionValue;
    }

    /**
     * Check if conditions "where" contain index key
     * For composite index key, it will return false if the conditions don't have all composite key.
     *
     * @return array|bool false or array
     *   ['name' => 'index_name', 'keysInfo' => ['hash' => 'hash_key', 'range' => 'range_key']]
     */
    protected function conditionsContainIndexKey()
    {
        if (empty($this->wheres)) {
            return false;
        }

        foreach ($this->model->getDynamoDbIndexKeys() as $name => $keysInfo) {
            $conditionKeys = array_pluck($this->wheres, 'column');
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
            $dynamoDbKey = $this->getSpecificDynamoDbKey($key, $this->model->getAttribute($key));

            if (!empty($dynamoDbKey)) {
                $keys = array_merge($keys, $dynamoDbKey);
            }
        }

        return $keys;
    }

    protected function getDynamoDbPrimaryKey()
    {
        return $this->getSpecificDynamoDbKey($this->model->getKeyName(), $this->model->getKey());
    }

    protected function getSpecificDynamoDbKey($keyName, $value)
    {
        if (is_null($value)) {
            return null;
        }

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

    private function cleanUpQuery($query)
    {
        return array_filter($query);
    }

    /**
     * @return DynamoDbModel
     */
    public function getModel()
    {
        return $this->model;
    }

    /**
     * @return DynamoDbClient
     */
    public function getClient()
    {
        return $this->client;
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

        $builder = $this;

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

        $result = $scope(...array_values($parameters)) ?: $this;

        // if (count((array) $query->wheres) > $originalWhereCount) {
        //     $this->addNewWheresWithinGroup($query, $originalWhereCount);
        // }

        return $result;
    }

    /**
     * Dynamically handle calls into the query instance.
     *
     * @param  string  $method
     * @param  array  $parameters
     * @return mixed
     */
    public function __call($method, $parameters)
    {
        if (method_exists($this->model, $scope = 'scope'.ucfirst($method))) {
            return $this->callScope([$this->model, $scope], $parameters);
        }

        return $this;
    }
}
