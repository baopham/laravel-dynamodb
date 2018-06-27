<?php

namespace BaoPham\DynamoDb\Tests\Facades;

use Aws\DynamoDb\DynamoDbClient;
use BaoPham\DynamoDb\DynamoDb\QueryBuilder;
use BaoPham\DynamoDb\Facades\DynamoDb;
use BaoPham\DynamoDb\Tests\DynamoDbTestCase;

class DynamoDbFacadeTest extends DynamoDbTestCase
{
    public function testStaticTableMethod()
    {
        $this->assertInstanceOf(QueryBuilder::class, DynamoDb::table('foobar'));
    }

    public function testStaticClientMethod()
    {
        $this->assertInstanceOf(DynamoDbClient::class, DynamoDb::client());
    }

    public function testStaticNewQuery()
    {
        $this->assertInstanceOf(QueryBuilder::class, DynamoDb::newQuery());
    }
}
