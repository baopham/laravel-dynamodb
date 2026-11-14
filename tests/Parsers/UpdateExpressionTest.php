<?php

namespace BaoPham\DynamoDb\Tests\Parsers;

use BaoPham\DynamoDb\Parsers\ExpressionAttributeNames;
use BaoPham\DynamoDb\Parsers\ExpressionAttributeValues;
use BaoPham\DynamoDb\Parsers\Placeholder;
use BaoPham\DynamoDb\Parsers\UpdateExpression;
use BaoPham\DynamoDb\Tests\DynamoDbTestCase;

class UpdateExpressionTest extends DynamoDbTestCase
{
    /**
     * @var UpdateExpression
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

    public function setUp(): void
    {
        parent::setUp();
        $this->names = new ExpressionAttributeNames();
        $this->values = new ExpressionAttributeValues();
        $this->parser = new UpdateExpression($this->names, $this->values, new Placeholder());
    }

    public function testParse()
    {
        $expression = $this->parser->remove(['foo.bar', 'a', 'b', 'hello[0]']);
        $this->assertEquals('REMOVE #a, #b, foo.bar, hello[0]', $expression);
        $this->assertEquals([
            '#a' => 'a',
            '#b' => 'b'
        ], $this->names->all());
    }

    public function testSetExpression()
    {
        $expression = $this->parser->set([
            'name' => 'Updated',
            'status' => true,
            'count' => 42,
            'notes' => null,
        ]);

        $this->assertEquals('SET #name = :a1, #status = :a2, #count = :a3, #notes = :a4', $expression);
        $this->assertEquals([
            '#name' => 'name',
            '#status' => 'status',
            '#count' => 'count',
            '#notes' => 'notes',
        ], $this->names->all());
        $this->assertEquals([
            ':a1' => ['S' => 'Updated'],
            ':a2' => ['BOOL' => true],
            ':a3' => ['N' => '42'],
            ':a4' => ['NULL' => true],
        ], $this->values->all());

        $this->parser->reset();
        $this->assertEmpty($this->names->all());
        $this->assertEmpty($this->values->all());
    }
}
