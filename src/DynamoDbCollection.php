<?php

namespace BaoPham\DynamoDb;

use Illuminate\Database\Eloquent\Collection;
use BaoPham\DynamoDb\ConditionAnalyzer\Index;

class DynamoDbCollection extends Collection
{
    /**
     * @var \BaoPham\DynamoDb\ConditionAnalyzer\Index
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
            return null;
        }

        $afterKey = $after->getKeys();

        $attributes = $this->conditionIndex ? $this->conditionIndex->columns() : [];

        foreach ($attributes as $attribute) {
            $afterKey[$attribute] = $after->getAttribute($attribute);
        }

        return $afterKey;
    }
}
