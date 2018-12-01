<?php

namespace BaoPham\DynamoDb\DynamoDb;

use BaoPham\DynamoDb\ComparisonOperator;
use BaoPham\DynamoDb\Concerns\HasParsers;
use BaoPham\DynamoDb\Facades\DynamoDb;
use BaoPham\DynamoDb\NotSupportedException;

/**
 * Class DynamoDbTransactItemQueryBuilder
 *
 * @package BaoPham\DynamoDb\DynamoDb
 */
class DynamoDbTransactItemQueryBuilder
{
    use HasParsers;

    /**
     * @var array
     */
    private $key;

    /**
     * @var array
     */
    public $conditions = [];

    /**
     * @var QueryBuilder
     */
    private $queryBuilder;

    /**
     * @var array
     */
    public $query = [];

    /**
     * @var string
     */
    public $type;

    public function __construct(QueryBuilder $queryBuilder)
    {
        $this->queryBuilder = $queryBuilder;
        $this->setupExpressions();
    }

    /**
     * @param array $key
     * @return $this
     */
    public function key(array $key) {
        $this->key = $key;

        return $this;
    }

    /**
     * @param $column
     * @param null $operator
     * @param null $value
     * @param string $boolean
     * @return $this|DynamoDbTransactItemQueryBuilder
     * @throws NotSupportedException
     */
    public function satisfy($column, $operator = null, $value = null, $boolean = 'and')
    {
        // If the column is an array, we will assume it is an array of key-value pairs
        // and can add them each as a where clause. We will maintain the boolean we
        // received when the method was called and pass it into the nested where.
        if (is_array($column)) {
            foreach ($column as $key => $value) {
                return $this->satisfy($key, '=', $value);
            }
        }

        // Here we will make some assumptions about the operator. If only 2 values are
        // passed to the method, we will assume that the operator is an equals sign
        // and keep going. Otherwise, we'll require the operator to be passed in.
        if (func_num_args() == 2) {
            list($value, $operator) = [$operator, '='];
        }

        if ($column instanceof \Closure) {
            return $this->satisfyNested($column, $boolean);
        }

        // If the given operator is not found in the list of valid operators we will
        // assume that the developer is just short-cutting the '=' operators and
        // we will set the operators to '=' and set the values appropriately.
        if (!ComparisonOperator::isValidOperator($operator)) {
            list($value, $operator) = [$operator, '='];
        }

        if ($value instanceof \Closure) {
            throw new NotSupportedException('Closure in where clause is not supported');
        }

        $this->conditions[] = [
            'column' => $column,
            'type' => ComparisonOperator::getDynamoDbOperator($operator),
            'value' => $value,
            'boolean' => $boolean,
        ];

        return $this;
    }

    /**
     * @param  string  $column
     * @param  string  $operator
     * @param  mixed   $value
     * @return $this
     */
    public function orSatisfy($column, $operator = null, $value = null)
    {
        return $this->satisfy($column, $operator, $value, 'or');
    }

    /**
     * Add a nested where statement to the query.
     *
     * @param  \Closure $callback
     * @param  string   $boolean
     * @return $this
     */
    public function satisfyNested(\Closure $callback, $boolean = 'and')
    {
        $query = new static($this->queryBuilder);
        call_user_func($callback, $query);

        if (count($query->conditions)) {
            $type = 'Nested';
            $column = null;
            $value = $query->conditions;
            $this->conditions[] = compact('column', 'type', 'value', 'boolean');
        }

        return $this;
    }

    /**
     * @param array $values
     */
    public function put(array $values)
    {
        $executableQuery = $this->queryBuilder
            ->setKey(DynamoDb::marshalItem($this->key))
            ->setConditionExpression($this->filterExpression->parse($this->conditions))
            ->setExpressionAttributeNames($this->expressionAttributeNames->all())
            ->setExpressionAttributeValues($this->expressionAttributeValues->all())
            ->setItem(DynamoDb::marshalItem($values))
            ->prepare();
        $this->prepare('Put', $executableQuery);
    }

    public function delete()
    {
        $executableQuery = $this->queryBuilder
            ->setKey(DynamoDb::marshalItem($this->key))
            ->setConditionExpression($this->filterExpression->parse($this->conditions))
            ->setExpressionAttributeNames($this->expressionAttributeNames->all())
            ->setExpressionAttributeValues($this->expressionAttributeValues->all())
            ->prepare();
        $this->prepare('Delete', $executableQuery);
    }

    public function get($columns = [])
    {
        $executableQuery = $this->queryBuilder
            ->setKey(DynamoDb::marshalItem($this->key))
            ->setConditionExpression($this->filterExpression->parse($this->conditions))
            ->setExpressionAttributeNames($this->expressionAttributeNames->all())
            ->setExpressionAttributeValues($this->expressionAttributeValues->all())
            ->setProjectionExpression($this->projectionExpression->parse($columns))
            ->prepare();
        $this->prepare('Get', $executableQuery);
    }

    public function check()
    {
        $executableQuery = $this->queryBuilder
            ->setKey(DynamoDb::marshalItem($this->key))
            ->setConditionExpression($this->filterExpression->parse($this->conditions))
            ->setExpressionAttributeNames($this->expressionAttributeNames->all())
            ->setExpressionAttributeValues($this->expressionAttributeValues->all())
            ->prepare();
        $this->prepare('ConditionCheck', $executableQuery);
    }

    private function prepare($type, ExecutableQuery $query)
    {
        $this->type = $type;
        $this->query = $query->query;
    }
}
