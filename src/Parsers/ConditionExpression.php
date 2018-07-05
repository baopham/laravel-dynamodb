<?php

namespace BaoPham\DynamoDb\Parsers;

use BaoPham\DynamoDb\ComparisonOperator;
use BaoPham\DynamoDb\NotSupportedException;
use BaoPham\DynamoDb\Facades\DynamoDb;

class ConditionExpression
{
    const OPERATORS = [
        ComparisonOperator::EQ => '%s = :%s',
        ComparisonOperator::LE => '%s <= :%s',
        ComparisonOperator::LT => '%s < :%s',
        ComparisonOperator::GE => '%s >= :%s',
        ComparisonOperator::GT => '%s > :%s',
        ComparisonOperator::BEGINS_WITH => 'begins_with(%s, :%s)',
        ComparisonOperator::BETWEEN => '(%s BETWEEN :%s AND :%s)',
        ComparisonOperator::CONTAINS => 'contains(%s, :%s)',
        ComparisonOperator::NOT_CONTAINS => 'NOT contains(%s, :%s)',
        ComparisonOperator::NULL => 'attribute_not_exists(%s)',
        ComparisonOperator::NOT_NULL => 'attribute_exists(%s)',
        ComparisonOperator::NE => '%s <> :%s',
        ComparisonOperator::IN => '%s IN (%s)',
    ];

    /**
     * @var ExpressionAttributeValues
     */
    protected $values;

    /**
     * @var ExpressionAttributeNames
     */
    protected $names;

    /**
     * @var Placeholder
     */
    protected $placeholder;

    public function __construct(
        Placeholder $placeholder,
        ExpressionAttributeValues $values,
        ExpressionAttributeNames $names
    ) {
        $this->placeholder = $placeholder;
        $this->values = $values;
        $this->names = $names;
    }

    /**
     * @param array $where
     *   [
     *     'column' => 'name',
     *     'type' => 'EQ',
     *     'value' => 'foo',
     *     'boolean' => 'and',
     *   ]
     *
     * @return string
     * @throws NotSupportedException
     */
    public function parse($where)
    {
        if (empty($where)) {
            return '';
        }

        $parsed = [];

        foreach ($where as $condition) {
            $boolean = array_get($condition, 'boolean');
            $value = array_get($condition, 'value');
            $type = array_get($condition, 'type');

            $prefix = '';

            if (count($parsed) > 0) {
                $prefix = strtoupper($boolean) . ' ';
            }

            if ($type === 'Nested') {
                $parsed[] = $prefix . $this->parseNestedCondition($value);
                continue;
            }

            $parsed[] = $prefix . $this->parseCondition(
                array_get($condition, 'column'),
                $type,
                $value
            );
        }

        return implode(' ', $parsed);
    }

    public function reset()
    {
        $this->placeholder->reset();
        $this->names->reset();
        $this->values->reset();
    }

    protected function getSupportedOperators()
    {
        return static::OPERATORS;
    }

    protected function parseNestedCondition(array $conditions)
    {
        return '(' . $this->parse($conditions) . ')';
    }

    protected function parseCondition($name, $operator, $value)
    {
        $operators = $this->getSupportedOperators();

        if (empty($operators[$operator])) {
            throw new NotSupportedException("$operator is not supported");
        }

        $template = $operators[$operator];

        $this->names->set($name);

        if ($operator === ComparisonOperator::BETWEEN) {
            return $this->parseBetweenCondition($name, $value, $template);
        }

        if ($operator === ComparisonOperator::IN) {
            return $this->parseInCondition($name, $value, $template);
        }

        if ($operator === ComparisonOperator::NULL || $operator === ComparisonOperator::NOT_NULL) {
            return $this->parseNullCondition($name, $template);
        }

        $placeholder = $this->placeholder->next();

        $this->values->set($placeholder, DynamoDb::marshalValue($value));

        return sprintf($template, $this->names->placeholder($name), $placeholder);
    }

    protected function parseBetweenCondition($name, $value, $template)
    {
        $first = $this->placeholder->next();

        $second = $this->placeholder->next();

        $this->values->set($first, DynamoDb::marshalValue($value[0]));

        $this->values->set($second, DynamoDb::marshalValue($value[1]));

        return sprintf($template, $this->names->placeholder($name), $first, $second);
    }

    protected function parseInCondition($name, $value, $template)
    {
        $valuePlaceholders = [];

        foreach ($value as $item) {
            $placeholder = $this->placeholder->next();

            $valuePlaceholders[] = ":" . $placeholder;

            $this->values->set($placeholder, DynamoDb::marshalValue($item));
        }

        return sprintf($template, $this->names->placeholder($name), implode(', ', $valuePlaceholders));
    }

    protected function parseNullCondition($name, $template)
    {
        return sprintf($template, $this->names->placeholder($name));
    }
}
