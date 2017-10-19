<?php

namespace BaoPham\DynamoDb\Tests\Parsers;

use BaoPham\DynamoDb\Parsers\ExpressionAttributeNames;
use BaoPham\DynamoDb\Parsers\UpdateExpression;
use BaoPham\DynamoDb\Tests\TestCase;

class UpdateExpressionTest extends TestCase
{
    /**
     * @var UpdateExpression
     */
    private $parser;

    /**
     * @var ExpressionAttributeNames
     */
    private $names;

    public function setUp()
    {
        parent::setUp();
        $this->names = new ExpressionAttributeNames();
        $this->parser = new UpdateExpression($this->names);
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
}
