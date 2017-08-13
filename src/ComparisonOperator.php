<?php

namespace BaoPham\DynamoDb;

/**
 * Class DynamoDbOperator.
 */
class ComparisonOperator
{
    const EQ = 'EQ';
    const GT = 'GT';
    const GE = 'GE';
    const LT = 'LT';
    const LE = 'LE';
    const IN = 'IN';
    const NE = 'NE';
    const BEGINS_WITH = 'BEGINS_WITH';
    const BETWEEN = 'BETWEEN';
    const NOT_CONTAINS = 'NOT_CONTAINS';
    const CONTAINS = 'CONTAINS';
    const NULL = 'NULL';
    const NOT_NULL = 'NOT_NULL';

    public static function getOperatorMapping()
    {
        return [
            '=' => static::EQ,
            '>' => static::GT,
            '>=' => static::GE,
            '<' => static::LT,
            '<=' => static::LE,
            'in' => static::IN,
            '!=' => static::NE,
            'begins_with' => static::BEGINS_WITH,
            'between' => static::BETWEEN,
            'not_contains' => static::NOT_CONTAINS,
            'contains' => static::CONTAINS,
            'null' => static::NULL,
            'not_null' => static::NOT_NULL,
        ];
    }

    public static function getSupportedOperators()
    {
        return array_keys(static::getOperatorMapping());
    }

    public static function isValidOperator($operator)
    {
        $operator = strtolower($operator);

        $mapping = static::getOperatorMapping();

        return isset($mapping[$operator]);
    }

    public static function getDynamoDbOperator($operator)
    {
        $mapping = static::getOperatorMapping();

        $operator = strtolower($operator);

        return $mapping[$operator];
    }

    public static function getQuerySupportedOperators($isRangeKey = false)
    {
        if ($isRangeKey) {
            return [
                static::EQ,
                static::LE,
                static::LT,
                static::GE,
                static::GT,
                static::BEGINS_WITH,
                static::BETWEEN,
            ];
        }

        return [static::EQ];
    }

    public static function isValidQueryOperator($operator, $isRangeKey = false)
    {
        $dynamoDbOperator = static::getDynamoDbOperator($operator);

        return static::isValidQueryDynamoDbOperator($dynamoDbOperator, $isRangeKey);
    }

    public static function isValidQueryDynamoDbOperator($dynamoDbOperator, $isRangeKey = false)
    {
        return in_array($dynamoDbOperator, static::getQuerySupportedOperators($isRangeKey));
    }

    public static function is($op, $dynamoDbOperator)
    {
        $mapping = static::getOperatorMapping();
        return $mapping[strtolower($op)] === $dynamoDbOperator;
    }
}
