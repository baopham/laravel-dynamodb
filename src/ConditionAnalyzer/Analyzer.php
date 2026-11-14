<?php

namespace BaoPham\DynamoDb\ConditionAnalyzer;

use BaoPham\DynamoDb\ComparisonOperator;
use BaoPham\DynamoDb\DynamoDbModel;
use BaoPham\DynamoDb\H;
use Illuminate\Support\Arr;

/**
 * Class ConditionAnalyzer
 * @package BaoPham\DynamoDb\ConditionAnalyzer
 *
 * Usage:
 *
 * $analyzer = with(new Analyzer)
 *  ->on($model)
 *  ->withIndex($index)
 *  ->analyze($conditions);
 *
 * $analyzer->isExactSearch();
 * $analyzer->keyConditions();
 * $analyzer->filterConditions();
 * $analyzer->index();
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

        if (count($this->conditions) !== count($this->model->getKeyNames())) {
            return false;
        }

        foreach ($this->conditions as $condition) {
            if (Arr::get($condition, 'type') !== ComparisonOperator::EQ) {
                return false;
            }

            if (array_search(Arr::get($condition, 'column'), $this->model->getKeyNames()) === false) {
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

        if (!$this->hasValidQueryOperator(...$keyNames)) {
            return null;
        }

        return $conditions;
    }

    public function identifierConditionValues()
    {
        $idConditions = $this->identifierConditions();

        if (!$idConditions) {
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
        if (empty($this->conditions) || $this->hasOrCondition()) {
            return null;
        }

        $indexKeys = $this->model->getDynamoDbIndexKeys();
        $conditionKeys = Arr::pluck($this->conditions, 'column');

        if (isset($this->indexName)) {
            $keysInfo = Arr::get($indexKeys, $this->indexName);

            if (empty($keysInfo)) {
                return null;
            }

            $hash = Arr::get($keysInfo, 'hash');
            $range = Arr::get($keysInfo, 'range');

            if (!$hash || !in_array($hash, $conditionKeys, true) || !$this->hasValidQueryOperator($hash, $range)) {
                return null;
            }

            return new Index($this->indexName, $hash, $range);
        }

        $bestIndex = null;
        $bestScore = 0;

        foreach ($indexKeys as $name => $keysInfo) {
            $hash = Arr::get($keysInfo, 'hash');
            $range = Arr::get($keysInfo, 'range');

            if (!$hash || !in_array($hash, $conditionKeys, true)) {
                continue;
            }

            if (!$this->hasValidQueryOperator($hash, $range)) {
                continue;
            }

            $score = ($range && in_array($range, $conditionKeys, true)) ? 2 : 1;

            if ($score > $bestScore) {
                $bestScore = $score;
                $bestIndex = new Index($name, $hash, $range);
            }
        }

        if ($bestIndex && $bestScore === 1 && $this->matchesPrimaryPartitionKey($conditionKeys)) {
            return null;
        }

        return $bestIndex;
    }

    private function matchesPrimaryPartitionKey(array $conditionKeys)
    {
        $keyNames = $this->model->getKeyNames();
        $primaryHash = $keyNames[0] ?? null;

        return $primaryHash
            && in_array($primaryHash, $conditionKeys, true)
            && $this->hasValidQueryOperator($primaryHash);
    }

    private function hasOrCondition()
    {
        foreach ($this->conditions as $condition) {
            if (strtolower((string) Arr::get($condition, 'boolean')) === 'or') {
                return true;
            }
        }

        return false;
    }

    private function hasValidQueryOperator($hash, $range = null)
    {
        $hashConditionType = $this->getCondition($hash)['type'] ?? null;
        $validQueryOp = ComparisonOperator::isValidQueryDynamoDbOperator($hashConditionType);

        if ($validQueryOp && $range && $this->getCondition($range) !== null) {
            $rangeConditionType = $this->getCondition($range)['type'];
            $validQueryOp = ComparisonOperator::isValidQueryDynamoDbOperator(
                $rangeConditionType,
                true
            );
        }

        return $validQueryOp;
    }
}
