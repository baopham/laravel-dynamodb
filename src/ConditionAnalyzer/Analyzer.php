<?php

namespace Rennokki\DynamoDb\ConditionAnalyzer;

use Illuminate\Support\Arr;
use Rennokki\DynamoDb\ComparisonOperator;
use Rennokki\DynamoDb\DynamoDbModel;
use Rennokki\DynamoDb\H;

/**
 * Class ConditionAnalyzer.
 */
class Analyzer
{
    /**
     * @var DynamoDbModel
     */
    private $model;

    /**
     * @var array
     */
    private $conditions = [];

    /**
     * @var string
     */
    private $indexName;

    public function on(DynamoDbModel $model)
    {
        $this->model = $model;

        return $this;
    }

    public function withIndex($index)
    {
        $this->indexName = $index;

        return $this;
    }

    public function analyze($conditions)
    {
        $this->conditions = $conditions;

        return $this;
    }

    public function isExactSearch()
    {
        if (empty($this->conditions)) {
            return false;
        }

        if (empty($this->identifierConditions())) {
            return false;
        }

        foreach ($this->conditions as $condition) {
            if (Arr::get($condition, 'type') !== ComparisonOperator::EQ) {
                return false;
            }
        }

        return true;
    }

    /**
     * @return Index|null
     */
    public function index()
    {
        return $this->getIndex();
    }

    public function keyConditions()
    {
        $index = $this->getIndex();

        if ($index) {
            return $this->getConditions($index->columns());
        }

        return $this->identifierConditions();
    }

    public function filterConditions()
    {
        $keyConditions = $this->keyConditions() ?: [];

        return array_filter($this->conditions, function ($condition) use ($keyConditions) {
            return array_search($condition, $keyConditions) === false;
        });
    }

    public function identifierConditions()
    {
        $keyNames = $this->model->getKeyNames();

        $conditions = $this->getConditions($keyNames);

        if (! $this->hasValidQueryOperator(...$keyNames)) {
            return;
        }

        return $conditions;
    }

    public function identifierConditionValues()
    {
        $idConditions = $this->identifierConditions();

        if (! $idConditions) {
            return [];
        }

        $values = [];

        foreach ($idConditions as $condition) {
            $values[$condition['column']] = $condition['value'];
        }

        return $values;
    }

    /**
     * @param $column
     *
     * @return array
     */
    private function getCondition($column)
    {
        return H::array_first($this->conditions, function ($condition) use ($column) {
            return $condition['column'] === $column;
        });
    }

    /**
     * @param $columns
     *
     * @return array
     */
    private function getConditions($columns)
    {
        return array_filter($this->conditions, function ($condition) use ($columns) {
            return in_array($condition['column'], $columns);
        });
    }

    /**
     * @return Index|null
     */
    private function getIndex()
    {
        if (empty($this->conditions)) {
            return;
        }

        $index = null;

        foreach ($this->model->getDynamoDbIndexKeys() as $name => $keysInfo) {
            $conditionKeys = Arr::pluck($this->conditions, 'column');
            $keys = array_values($keysInfo);

            if (count(array_intersect($conditionKeys, $keys)) === count($keys)) {
                if (! isset($this->indexName) || $this->indexName === $name) {
                    $index = new Index(
                        $name,
                        Arr::get($keysInfo, 'hash'),
                        Arr::get($keysInfo, 'range')
                    );

                    break;
                }
            }
        }

        if ($index && ! $this->hasValidQueryOperator($index->hash, $index->range)) {
            $index = null;
        }

        return $index;
    }

    private function hasValidQueryOperator($hash, $range = null)
    {
        $hashCondition = $this->getCondition($hash);

        $validQueryOp = ComparisonOperator::isValidQueryDynamoDbOperator($hashCondition['type']);

        if ($validQueryOp && $range) {
            $rangeCondition = $this->getCondition($range);

            $validQueryOp = ComparisonOperator::isValidQueryDynamoDbOperator(
                $rangeCondition['type'],
                true
            );
        }

        return $validQueryOp;
    }
}
