<?php

namespace BaoPham\DynamoDb\Tests;

use Carbon\Carbon;
use Illuminate\Support\Arr;
use Illuminate\Support\Str;

/**
 * Class DynamoDbTimestampTest
 *
 * @package BaoPham\DynamoDb\Tests
 */
class DynamoDbTimestampTest extends DynamoDbModelTestBase
{
    protected function getTestModel()
    {
        return new TimestampModel([]);
    }

    public function testCreateRecord()
    {
        Carbon::setTestNow(Carbon::create(2017, 06, 24, 5, 30, 0));
        $now = new Carbon;
        $this->testModel->id = Str::random(36);
        $this->testModel->name = 'Test Create';
        $this->testModel->count = 1;
        $this->testModel->save();

        $query = [
            'TableName' => $this->testModel->getTable(),
            'Key' => [
                'id' => ['S' => $this->testModel->id]
            ],
        ];

        $record = $this->getClient()->getItem($query)->toArray();

        $this->assertArrayHasKey('Item', $record);
        $this->assertEquals($this->testModel->created_at, $now);
    }

    public function testUpdateRecord()
    {
        Carbon::setTestNow(Carbon::create(2017, 03, 01, 8, 30, 0));
        $now = new Carbon();
        $seed = $this->seed();
        $seedId = Arr::get($seed, 'id.S');

        $newName = 'New Name';
        $model = $this->testModel->find($seedId);
        $updated = $model->update(['name' => $newName]);

        $this->assertTrue($updated);

        $query = [
            'TableName' => $model->getTable(),
            'Key' => [
                'id' => ['S' => $seedId]
            ],
        ];

        $record = $this->marshaler->unmarshalItem($this->getClient()->getItem($query)->get('Item'));

        $this->assertEquals($now, $model->updated_at);
        $this->assertEquals($now, Carbon::parse($record['updated_at']));
    }

    public function seed($attributes = [])
    {
        $item = [
            'id' => ['S' => Str::random(36)],
            'name' => ['S' => Str::random(36)],
            'description' => ['S' => Str::random(256)],
            'count' => ['N' => rand()],
            'author' => ['S' => Str::random()],
        ];

        $item = array_merge($item, $attributes);

        $this->getClient()->putItem([
            'TableName' => $this->testModel->getTable(),
            'Item' => $item,
        ]);

        return $item;
    }
}

// phpcs:disable PSR1.Classes.ClassDeclaration.MultipleClasses
class TimestampModel extends \BaoPham\DynamoDb\DynamoDbModel
{
    protected $fillable = ['name', 'description', 'count'];

    protected $table = 'test_model';

    protected $connection = 'test';

    public $timestamps = true;

    protected $dynamoDbIndexKeys = [
        'count_index' => [
            'hash' => 'count',
        ],
    ];
}
// phpcs:enable PSR1.Classes.ClassDeclaration.MultipleClasses
