<?php

namespace BaoPham\DynamoDb\Tests\Parsers;

use Aws\DynamoDb\Marshaler;
use BaoPham\DynamoDb\ComparisonOperator;
use BaoPham\DynamoDb\Parsers\ConditionExpression;
use BaoPham\DynamoDb\Parsers\ExpressionAttributeNames;
use BaoPham\DynamoDb\Parsers\ExpressionAttributeValues;
use BaoPham\DynamoDb\Parsers\Placeholder;
use BaoPham\DynamoDb\Tests\TestCase;

class ConditionExpressionTest extends TestCase
{
    /**
     * @var ConditionExpression
     */
    private $parser;

    /**
     * @var ExpressionAttributeNames
     */
    private $names;

    /**
     * @var ExpressionAttributeValues
     */
    private $values;

    public function setUp()
    {
        parent::setUp();
        $this->names = new ExpressionAttributeNames();
        $this->values = new ExpressionAttributeValues();
        $this->parser = new ConditionExpression(
            new Placeholder(),
            new Marshaler(),
            $this->values,
            $this->names
        );
    }

    public function testParse()
    {
        $where = [
            [
                'column' => 'name',
                'operator' => ComparisonOperator::EQ,
                'value' => 'foo',
                'boolean' => 'and',
            ],
            [
                'column' => 'count1',
                'operator' => ComparisonOperator::GT,
                'value' => 1,
                'boolean' => 'and',
            ],
            [
                'column' => 'count2',
                'operator' => ComparisonOperator::GE,
                'value' => 2,
                'boolean' => 'or',
            ],
            [
                'column' => 'count3',
                'operator' => ComparisonOperator::LT,
                'value' => 3,
                'boolean' => 'and',
            ],
            [
                'column' => 'count4',
                'operator' => ComparisonOperator::LE,
                'value' => 4,
                'boolean' => 'and',
            ],
            [
                'column' => 'description',
                'operator' => ComparisonOperator::BEGINS_WITH,
                'value' => 'hello world',
                'boolean' => 'and',
            ],
            [
                'column' => 'score',
                'operator' => ComparisonOperator::BETWEEN,
                'value' => [0, 100],
                'boolean' => 'and',
            ],
            [
                'column' => 'age',
                'operator' => ComparisonOperator::IN,
                'value' => [0, 20],
                'boolean' => 'and',
            ],
            [
                'column' => 'foo',
                'operator' => ComparisonOperator::CONTAINS,
                'value' => 'foobar',
                'boolean' => 'and',
            ],
            [
                'column' => 'bar',
                'operator' => ComparisonOperator::NOT_CONTAINS,
                'value' => 'foobar',
                'boolean' => 'or',
            ],
            [
                'column' => 'gender',
                'operator' => ComparisonOperator::NULL,
                'boolean' => 'and',
            ],
            [
                'column' => 'occupation',
                'operator' => ComparisonOperator::NOT_NULL,
                'boolean' => 'and',
            ],
            [
                'column' => 'retired',
                'operator' => ComparisonOperator::NE,
                'value' => true,
                'boolean' => 'or',
            ],
        ];

        $this->assertEquals(
            "#name = :a1 AND #count1 > :a2 OR #count2 >= :a3 AND #count3 < :a4 " .
            "AND #count4 <= :a5 AND begins_with(#description, :a6) AND (#score BETWEEN :a7 AND :a8) " .
            "AND #age IN (:a9, :a10) AND contains(#foo, :a11) OR NOT contains(#bar, :a12) " .
            "AND attribute_not_exists(#gender) AND attribute_exists(#occupation) OR #retired <> :a13",
            $this->parser->parse($where)
        );

        $this->assertEquals(['S' => 'foo'], $this->values->get(':a1'));
        $this->assertEquals(['N' => 1], $this->values->get(':a2'));
        $this->assertEquals(['N' => 2], $this->values->get(':a3'));
        $this->assertEquals(['N' => 3], $this->values->get(':a4'));
        $this->assertEquals(['N' => 4], $this->values->get(':a5'));
        $this->assertEquals(['S' => 'hello world'], $this->values->get(':a6'));
        $this->assertEquals(['N' => 0], $this->values->get(':a7'));
        $this->assertEquals(['N' => 100], $this->values->get(':a8'));
        $this->assertEquals(['N' => 0], $this->values->get(':a9'));
        $this->assertEquals(['N' => 20], $this->values->get(':a10'));
        $this->assertEquals(['S' => 'foobar'], $this->values->get(':a11'));
        $this->assertEquals(['S' => 'foobar'], $this->values->get(':a12'));
        $this->assertEquals(['BOOL' => true], $this->values->get(':a13'));

        $columns = array_pluck($where, 'column');

        foreach ($columns as $column) {
            $this->assertEquals($column, $this->names->get("#{$column}"));
        }
    }
}