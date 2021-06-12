<?php

namespace BaoPham\DynamoDb\Parsers;

use BaoPham\DynamoDb\ComparisonOperator;
use Illuminate\Support\Arr;

class KeyConditionExpression extends ConditionExpression
{
    protected function getSupportedOperators()
    {
        return Arr::only(static::OPERATORS, [
            ComparisonOperator::EQ,
            ComparisonOperator::LE,
            ComparisonOperator::LT,
            ComparisonOperator::GE,
            ComparisonOperator::GT,
            ComparisonOperator::BEGINS_WITH,
            ComparisonOperator::BETWEEN,
        ]);
    }
}
