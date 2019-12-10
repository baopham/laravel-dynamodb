<?php

namespace Rennokki\DynamoDb\Tests\Facades;

use Aws\DynamoDb\DynamoDbClient;
use Rennokki\DynamoDb\DynamoDb\QueryBuilder;
use Rennokki\DynamoDb\Facades\DynamoDb;
use Rennokki\DynamoDb\Tests\DynamoDbTestCase;

class DynamoDbFacadeTest extends DynamoDbTestCase
{
    public function testStaticTableMethod()
    {
        $this->assertInstanceOf(QueryBuilder::class, DynamoDb::table('foobar'));
    }

    public function testStaticClientMethod()
    {
        $this->assertInstanceOf(DynamoDbClient::class, DynamoDb::client('test'));
    }

    public function testStaticNewQuery()
    {
        $this->assertInstanceOf(QueryBuilder::class, DynamoDb::newQuery());
    }

    public function testStaticMarshalItem()
    {
        $this->assertEquals(['id' => ['S' => 'hello']], DynamoDb::marshalItem(['id' => 'hello']));
    }

    public function testStaticUnmarshalItem()
    {
        $this->assertEquals(['id' => 'hello'], DynamoDb::unmarshalItem(['id' => ['S' => 'hello']]));
    }

    public function testStaticMarshalValue()
    {
        $this->assertEquals(['S' => 'hello'], DynamoDb::marshalValue('hello'));
    }

    public function testStaticUnmarshalValue()
    {
        $this->assertEquals('hello', DynamoDb::unmarshalValue(['S' => 'hello']));
    }
}
