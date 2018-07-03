<?php

namespace BaoPham\DynamoDb;

use Illuminate\Database\Eloquent\Collection;

class DynamoDbCollection extends Collection
{
    private $conditionIndexes = null;

    public function __construct(array $items = [], $conditionIndexes = null)
    {
        parent::__construct($items);

        $this->conditionIndexes = $conditionIndexes;
    }

    public function lastKey()
    {
        $after = $this->last();

        if (empty($after)) {
            return null;
        }

        $afterKey = $after->getKeys();

        $conditionIndexes = $this->conditionIndexes ?: [];

        foreach ($conditionIndexes as $index) {
            $afterKey[$index] = $after->getAttribute($index);
        }

        return $afterKey;
    }
}
