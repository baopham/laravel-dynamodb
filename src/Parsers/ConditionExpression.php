<?php

namespace BaoPham\DynamoDb\Parsers;

use Aws\DynamoDb\Marshaler;
use BaoPham\DynamoDb\ComparisonOperator;
use BaoPham\DynamoDb\NotSupportedException;

class ConditionExpression
{
    const OPERATORS = [
        ComparisonOperator::EQ => '#%s = :%s',
        ComparisonOperator::LE => '#%s <= :%s',
        ComparisonOperator::LT => '#%s < :%s',
        ComparisonOperator::GE => '#%s >= :%s',
        ComparisonOperator::GT => '#%s > :%s',
        ComparisonOperator::BEGINS_WITH => 'begins_with(#%s, :%s)',
        ComparisonOperator::BETWEEN => '(#%s BETWEEN :%s AND :%s)',
        ComparisonOperator::CONTAINS => 'contains(#%s, :%s)',
        ComparisonOperator::NOT_CONTAINS => 'NOT contains(#%s, :%s)',
        ComparisonOperator::NULL => 'attribute_not_exists(#%s)',
        ComparisonOperator::NOT_NULL => 'attribute_exists(#%s)',
        ComparisonOperator::NE => '#%s <> :%s',
        ComparisonOperator::IN => '#%s IN (%s)',
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

    /**
     * @var Marshaler
     */
    protected $marshaler;

    public function __construct(
        Placeholder $placeholder,
        Marshaler $marshaler,
        ExpressionAttributeValues $values,
        ExpressionAttributeNames $names
    ) {
        $this->placeholder = $placeholder;
        $this->marshaler = $marshaler;
        $this->values = $values;
        $this->names = $names;
    }

    /**
     * @param array $where
     *   [
     *     'column' => 'name',
     *     'operator' => 'EQ',
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

            $prefix = '';

            if (count($parsed) > 0) {
                $prefix = strtoupper($boolean) . ' ';
            }

            $parsed[] = $prefix . $this->parseCondition(
                array_get($condition, 'column'),
                array_get($condition, 'operator'),
                array_get($condition, 'value')
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

    protected function parseCondition($name, $operator, $value)
    {
        $operators = $this->getSupportedOperators();

        if (empty($operators[$operator])) {
            throw new NotSupportedException("$operator is not supported for KeyConditionExpression");
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

        $this->values->set($placeholder, $this->marshaler->marshalValue($value));

        return sprintf($template, $name, $placeholder);
    }

    protected function parseBetweenCondition($name, $value, $template)
    {
        $first = $this->placeholder->next();

        $second = $this->placeholder->next();

        $this->values->set($first, $this->marshaler->marshalValue($value[0]));

        $this->values->set($second, $this->marshaler->marshalValue($value[1]));

        return sprintf($template, $name, $first, $second);
    }

    protected function parseInCondition($name, $value, $template)
    {
        $valuePlaceholders = [];

        foreach ($value as $item) {
            $placeholder = $this->placeholder->next();

            $valuePlaceholders[] = ":" . $placeholder;

            $this->values->set($placeholder, $this->marshaler->marshalValue($item));
        }

        return sprintf($template, $name, implode(', ', $valuePlaceholders));
    }

    protected function parseNullCondition($name, $template)
    {
        return sprintf($template, $name);
    }
}
