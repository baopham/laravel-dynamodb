<?php

namespace Rennokki\DynamoDb;

use Illuminate\Database\Eloquent\Collection;
use Rennokki\DynamoDb\ConditionAnalyzer\Index;

class DynamoDbCollection extends Collection
{
    /**
     * @var \Rennokki\DynamoDb\ConditionAnalyzer\Index
     */
    private $conditionIndex = null;

    public function __construct(array $items = [], Index $conditionIndex = null)
    {
        parent::__construct($items);

        $this->conditionIndex = $conditionIndex;
    }

    public function lastKey()
    {
        $after = $this->last();

        if (empty($after)) {
            return;
        }

        $afterKey = $after->getKeys();

        $attributes = $this->conditionIndex ? $this->conditionIndex->columns() : [];

        foreach ($attributes as $attribute) {
            $afterKey[$attribute] = $after->getAttribute($attribute);
        }

        return $afterKey;
    }
}
