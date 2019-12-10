<?php

namespace Rennokki\DynamoDb\Tests\DynamoDb;

use Aws\DynamoDb\DynamoDbClient;
use Aws\DynamoDb\Marshaler;
use Rennokki\DynamoDb\DynamoDb\DynamoDbManager;
use Rennokki\DynamoDb\DynamoDbClientInterface;
use Rennokki\DynamoDb\Tests\DynamoDbTestCase;

class DynamoDbManagerTest extends DynamoDbTestCase
{
    /**
     * @var DynamoDbManager
     */
    protected $manager;

    /**
     * @var \PHPUnit\Framework\MockObject\MockObject
     */
    protected $mockedClient;

    public function setUp(): void
    {
        parent::setUp();

        $this->mockedClient = $this
            ->getMockBuilder(DynamoDbClient::class)
            ->disableOriginalConstructor()
            ->setMethods(['putItem', 'updateItem', 'deleteItem', 'scan', 'query', 'batchWriteItem'])
            ->getMock();

        $service = $this->getMockBuilder(DynamoDbClientInterface::class)->getMock();
        $service->method('getMarshaler')->willReturn(new Marshaler());
        $service->method('getClient')->willReturn($this->mockedClient);

        $this->manager = with(new DynamoDbManager($service));
    }

    public function testPutItem()
    {
        $this->mockedClient->expects($this->once())
            ->method('putItem')
            ->with([
                'TableName' => 'articles',
                'Item' => [
                    'id' => ['S' => 'ae025ed8'],
                    'author_name' => ['S' => 'Bao'],
                ],
            ]);

        $this->manager->table('articles')
            ->setItem($this->manager->marshalItem(['id' => 'ae025ed8', 'author_name' => 'Bao']))
            ->prepare()
            ->putItem();
    }

    public function testUpdateItem()
    {
        $this->mockedClient->expects($this->once())
            ->method('updateItem')
            ->with([
                'TableName' => 'articles',
                'Key' => ['id' => ['S' => 'ae025ed8']],
                'UpdateExpression' => 'REMOVE #c, #t',
                'ExpressionAttributeNames' => ['#c' => 'comments', '#t' => 'tags'],
            ]);

        $this->manager->table('articles')
            ->setKey($this->manager->marshalItem(['id' => 'ae025ed8']))
            ->setUpdateExpression('REMOVE #c, #t')
            ->setExpressionAttributeName('#c', 'comments')
            ->setExpressionAttributeName('#t', 'tags')
            ->prepare()
            ->updateItem();
    }

    public function testDeleteItem()
    {
        $this->mockedClient->expects($this->once())
            ->method('deleteItem')
            ->with([
                'TableName' => 'articles',
                'Key' => ['id' => ['S' => 'ae025ed8']],
            ]);

        $this->manager->table('articles')
            ->setKey($this->manager->marshalItem(['id' => 'ae025ed8']))
            ->prepare()
            ->deleteItem();
    }

    public function testScan()
    {
        $this->mockedClient->expects($this->once())
            ->method('scan')
            ->with([
                'TableName' => 'articles',
                'Limit' => 2,
                'FilterExpression' => '#c > :count AND #t IN :tags',
                'ExpressionAttributeNames' => ['#c' => 'comments', '#t' => 'tags'],
                'ExpressionAttributeValues' => [
                    ':count' => ['N' => 2],
                    ':tags' => ['L' => [['S' => 'a'], ['S' => 'b']]],
                ],
            ]);

        $this->manager->table('articles')
            ->setLimit(2)
            ->setFilterExpression('#c > :count AND #t IN :tags')
            ->setExpressionAttributeName('#c', 'comments')
            ->setExpressionAttributeName('#t', 'tags')
            ->setExpressionAttributeValue(':count', $this->manager->marshalValue(2))
            ->setExpressionAttributeValue(':tags', $this->manager->marshalValue(['a', 'b']))
            ->prepare()
            ->scan();
    }

    public function testQuery()
    {
        $this->mockedClient->expects($this->once())
            ->method('query')
            ->with([
                'TableName' => 'articles',
                'Limit' => 2,
                'IndexName' => 'author_name',
                'KeyConditionExpression' => '#name = :name',
                'ExpressionAttributeNames' => ['#name' => 'author_name'],
                'ExpressionAttributeValues' => [
                    ':name' => ['S' => 'Bao'],
                ],
            ]);

        $this->manager->table('articles')
            ->setIndexName('author_name')
            ->setLimit(2)
            ->setKeyConditionExpression('#name = :name')
            ->setExpressionAttributeName('#name', 'author_name')
            ->setExpressionAttributeValue(':name', $this->manager->marshalValue('Bao'))
            ->prepare()
            ->query();
    }

    public function testBatchWriteItem()
    {
        $this->mockedClient->expects($this->once())
            ->method('batchWriteItem')
            ->with(['RequestItems' => ['articles' => []]]);

        $this->manager->newQuery()
            ->setRequestItems(['articles' => []])
            ->prepare()
            ->batchWriteItem();
    }

    public function testClient()
    {
        $this->assertInstanceOf(DynamoDbClient::class, $this->manager->client());
    }
}
