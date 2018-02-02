<?php

namespace BaoPham\DynamoDb;

use Illuminate\Database\Eloquent\Collection;

class DynamoDbCollection extends Collection
{
    protected $lastEvaluatedKey = null;
    protected $conditionIndexKeys = null;
    protected $model = null;

    public function __construct(array $models = [], $lastEvaluatedKey = null, $conditionIndexKeys = null)
    {
        parent::__construct($models);
        $this->lastEvaluatedKey = $lastEvaluatedKey;
        $this->conditionIndexKeys = $conditionIndexKeys;

        if (!$this->isEmpty()) {
            $class = get_class($this->first());
            $this->model = new $class;
        }
    }

    public function getLastEvaluatedKey()
    {
        if (!empty($this->lastEvaluatedKey)) {
            return $this->lastEvaluatedKey;
        }

        $after = $this->last();

        if (empty($after)) {
            return null;
        }

        $afterKey = $after->getKeys();

        if ($this->conditionIndexKeys) {
            $columns = array_values($conditionIndexKeys['keysInfo']);
            foreach ($columns as $column) {
                $afterKey[$column] = $after->getAttribute($column);
            }
        }

        return $this->model->unmarshalValue($this->getDynamoDbKey($afterKey));
    }

    /**
     * Return key for DynamoDb query.
     *
     * @param array|null $modelKeys
     * @return array
     *
     * e.g.
     * [
     *   'id' => ['S' => 'foo'],
     * ]
     *
     * or
     *
     * [
     *   'id' => ['S' => 'foo'],
     *   'id2' => ['S' => 'bar'],
     * ]
     */
    protected function getDynamoDbKey($modelKeys = null)
    {
        $modelKeys = $modelKeys ?: $this->model->getKeys();

        $keys = [];

        foreach ($modelKeys as $key => $value) {
            if (is_null($value)) {
                continue;
            }
            $keys[$key] = $this->model->marshalValue($value);
        }

        return $keys;
    }
}
