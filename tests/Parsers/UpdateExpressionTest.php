<?php

namespace Rennokki\DynamoDb\Tests\Parsers;

use PHPUnit\Framework\TestCase;
use Rennokki\DynamoDb\Parsers\ExpressionAttributeNames;
use Rennokki\DynamoDb\Parsers\UpdateExpression;

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

    public function setUp(): void
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
            '#b' => 'b',
        ], $this->names->all());
    }
}
