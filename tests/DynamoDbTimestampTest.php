<?php

namespace BaoPham\DynamoDb\Tests;

use Carbon\Carbon;

/**
 * Class DynamoDbTimestampTest
 *
 * @package BaoPham\DynamoDb\Tests
 */
class DynamoDbTimestampTest extends ModelTest
{
    protected function getTestModel()
    {
        return new TimestampModel([]);
    }

    public function testCreateRecord()
    {
        Carbon::setTestNow(Carbon::create(2017, 06, 24, 5, 30, 0));
        $now = new Carbon;
        $this->testModel->id = str_random(36);
        $this->testModel->name = 'Test Create';
        $this->testModel->count = 1;
        $this->testModel->save();

        $query = [
            'TableName' => $this->testModel->getTable(),
            'Key' => [
                'id' => ['S' => $this->testModel->id]
            ],
        ];

        $record = $this->dynamoDbClient->getItem($query)->toArray();

        $this->assertArrayHasKey('Item', $record);
        $this->assertEquals($this->testModel->created_at, $now);
    }

    public function testUpdateRecord()
    {
        Carbon::setTestNow(Carbon::create(2017, 03, 01, 8, 30, 0));
        $now = new Carbon;
        $seed = $this->seed();
        $seedId = array_get($seed, 'id.S');

        $newName = 'New Name';
        $this->testModel = $this->testModel->find($seedId);
        $updated = $this->testModel->update(['name' => $newName]);

        $this->assertTrue($updated);

        $query = [
            'TableName' => $this->testModel->getTable(),
            'Key' => [
                'id' => ['S' => $seedId]
            ],
        ];

        $record = $this->dynamoDbClient->getItem($query)->toArray();

        $this->assertEquals($this->testModel->updated_at, $now);
    }

    protected function seed($attributes = [])
    {
        $item = [
            'id' => ['S' => str_random(36)],
            'name' => ['S' => str_random(36)],
            'description' => ['S' => str_random(256)],
            'count' => ['N' => rand()],
            'author' => ['S' => str_random()],
        ];

        $item = array_merge($item, $attributes);

        $this->dynamoDbClient->putItem([
            'TableName' => $this->testModel->getTable(),
            'Item' => $item,
        ]);

        return $item;
    }
}

class TimestampModel extends \BaoPham\DynamoDb\DynamoDbModel
{
    protected $fillable = ['name', 'description', 'count'];

    protected $table = 'test_model';

    public $timestamps = true;

    protected $dynamoDbIndexKeys = [
        'count_index' => [
            'hash' => 'count',
        ],
    ];
}
