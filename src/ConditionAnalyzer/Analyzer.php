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
        if (empty($this->conditions)) {
            return null;
        }

        $indexKeys = $this->model->getDynamoDbIndexKeys();
        $conditionKeys = Arr::pluck($this->conditions, 'column');

        $candidates = [];
        foreach ($indexKeys as $name => $keysInfo) {
            if (empty($keysInfo['hash'])) {
                // no hash key? Yeah, no index
                continue;
            }

            if (is_array($keysInfo['hash'])) {
                // check each hash component, to ensure *all* match.
                foreach ($keysInfo['hash'] as $hash_component) {
                    if(!in_array($hash_component, $conditionKeys)) {
                        //one of the hash components is *not* in the list of conditions
                        //so this index won't work.
                        continue 2; //we need to break out of *this* foreach as well as the parent one
                    }
                }
            } else if (!in_array($keysInfo['hash'], $conditionKeys)) {
                // no match on hash key - this index won't work, period
                continue;
            }

            $weight = 1;

            if (!empty($keysInfo['range'])) {
                if (is_array($keysInfo['range'])) {
                    //we need to walk elements left-to-right, stopping as soon as there's no match
                    // the "weight" we will get is dependent upon how many range elements there are.
                    // if all match, it's +1. If none match, it's +0. If you have 4 elements, and
                    // only *3* match (from left-to-right), then it's +0.75
                    $component_count = count($keysInfo['range']);
                    $matched_count = 0;
                    foreach($keysInfo['range'] as $range_component) {
                        if(!in_array($range_component, $conditionKeys)) {
                            break;
                        }
                        $matched_count++;
                    }
                    $weight += ($matched_count / $component_count);
                } else if (in_array($keysInfo['range'], $conditionKeys)) {
                    $weight++;
                }
            }
            // lack of range key doesn't mean this isn't a candidate

            $candidates[$name] = $weight;
        }

        // now, run through the candidates and find the one with the highest 'weight' - use that one
        arsort($candidates, SORT_NUMERIC);

        $index = null;
        foreach ($candidates as $name => $weight) {
            // still allow an overridden index to take precedence
            if (!isset($this->indexName) || $this->indexName === $name) {
                $keysInfo = $indexKeys[$name];
                $index = new Index(
                    $name,
                    Arr::get($keysInfo, 'hash'),
                    Arr::get($keysInfo, 'range')
                );
                break; // got one! Don't need to go through the rest
            }
        }

        if ($index && !$this->hasValidQueryOperator($index->hash, $index->range)) {
            $index = null;
        }

        return $index;
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
