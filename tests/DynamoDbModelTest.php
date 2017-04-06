<?php

namespace BaoPham\DynamoDb\Tests;

/**
 * Class DynamoDbModelTest
 *
 * @package BaoPham\DynamoDb\Tests
 */
class DynamoDbModelTest extends ModelTest
{
    protected function getTestModel()
    {
        return new TestModel([]);
    }

    public function testCreateRecord()
    {
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
        $this->assertEquals($this->testModel->id, $record['Item']['id']['S']);
    }

    public function testFindRecord()
    {
        $seed = $this->seed();
        $seedId = array_get($seed, 'id.S');
        $seedName = array_get($seed, 'name.S');

        $item = $this->testModel->find($seedId);

        $this->assertNotEmpty($item);
        $this->assertEquals($seedId, $item->id);
        $this->assertEquals($seedName, $item->name);
    }

    public function testGetAllRecords()
    {
        $this->seed();
        $this->seed();

        $items = $this->testModel->all()->toArray();

        $this->assertEquals(2, count($items));
    }

    public function testUpdateRecord()
    {
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

        $this->assertEquals($newName, array_get($record, 'Item.name.S'));
    }

    public function testSaveRecord()
    {
        $seed = $this->seed();
        $seedId = array_get($seed, 'id.S');

        $newName = 'New Name to be saved';
        $this->testModel = $this->testModel->find($seedId);
        $this->testModel->name = $newName;

        $this->testModel->save();

        $query = [
            'TableName' => $this->testModel->getTable(),
            'Key' => [
                'id' => ['S' => $seedId]
            ],
        ];

        $record = $this->dynamoDbClient->getItem($query)->toArray();

        $this->assertEquals($newName, array_get($record, 'Item.name.S'));
    }

    public function testDeleteRecord()
    {
        $seed = $this->seed();
        $seedId = array_get($seed, 'id.S');

        $this->testModel->find($seedId)->delete();

        $query = [
            'TableName' => $this->testModel->getTable(),
            'Key' => [
                'id' => ['S' => $seedId]
            ],
        ];

        $record = $this->dynamoDbClient->getItem($query)->toArray();

        $this->assertArrayNotHasKey('Item', $record);
    }

    public function testGetAllRecordsWithEqualCondition()
    {
        $this->seed(['count' => ['N' => 10]]);
        $this->seed(['count' => ['N' => 10]]);
        $this->seed(['count' => ['N' => 10]]);

        $items = $this->testModel->where('count', 10)->get()->toArray();

        $this->assertEquals(3, count($items));
    }

    public function testGetAllRecordsWithNonEqualCondition()
    {
        $this->seed(['count' => ['N' => 10]]);
        $this->seed(['count' => ['N' => 11]]);
        $this->seed(['count' => ['N' => 11]]);

        $items = $this->testModel->where('count', '!=', 10)->get()->toArray();

        $this->assertEquals(2, count($items));
    }

    public function testGetAllRecordsWithGTCondition()
    {
        $this->seed(['count' => ['N' => 10]]);
        $this->seed(['count' => ['N' => 11]]);
        $this->seed(['count' => ['N' => 11]]);

        $items = $this->testModel->where('count', '>', 10)->get()->toArray();

        $this->assertEquals(2, count($items));
    }

    public function testGetAllRecordsWithLTCondition()
    {
        $this->seed(['count' => ['N' => 10]]);
        $this->seed(['count' => ['N' => 9]]);
        $this->seed(['count' => ['N' => 11]]);

        $items = $this->testModel->where('count', '<', 10)->get()->toArray();

        $this->assertEquals(1, count($items));
    }

    public function testGetAllRecordsWithGECondition()
    {
        $this->seed(['count' => ['N' => 10]]);
        $this->seed(['count' => ['N' => 9]]);
        $this->seed(['count' => ['N' => 11]]);

        $items = $this->testModel->where('count', '>=', 10)->get()->toArray();

        $this->assertEquals(2, count($items));
    }

    public function testGetAllRecordsWithLECondition()
    {
        $this->seed(['count' => ['N' => 10]]);
        $this->seed(['count' => ['N' => 9]]);
        $this->seed(['count' => ['N' => 11]]);

        $items = $this->testModel->where('count', '<=', 10)->get()->toArray();

        $this->assertEquals(2, count($items));
    }

    public function testGetAllRecordsWithBeginsWithOperator()
    {
        $this->seed(['description' => ['S' => 'Foo_1']]);
        $this->seed(['description' => ['S' => 'Foo_2']]);
        $this->seed(['description' => ['S' => 'Bar_Foo']]);

        $items = $this->testModel->where('description', 'BEGINS_WITH', 'Foo')->get()->toArray();

        $this->assertEquals(2, count($items));
    }

    public function testGetAllRecordsWithContainsOperator()
    {
        $this->seed(['description' => ['L' => [['S' => 'foo'], ['S' => 'bar']]]]);
        $this->seed(['description' => ['L' => [['S' => 'foo'], ['S' => 'bar2']]]]);

        $items = $this->testModel->where('description', 'CONTAINS', 'foo')->get()->toArray();

        $this->assertEquals(2, count($items));

        $items = $this->testModel->where('description', 'CONTAINS', 'bar2')->get()->toArray();

        $this->assertEquals(1, count($items));
    }

    public function testGetAllRecordsWithNotContainsOperator()
    {
        $this->seed(['description' => ['L' => [['S' => 'foo'], ['S' => 'bar']]]]);
        $this->seed(['description' => ['L' => [['S' => 'foo'], ['S' => 'bar2']]]]);

        $items = $this->testModel->where('description', 'NOT_CONTAINS', 'foo')->get()->toArray();

        $this->assertEquals(0, count($items));

        $items = $this->testModel->where('description', 'NOT_CONTAINS', 'foobar')->get()->toArray();

        $this->assertEquals(2, count($items));
    }

    public function testGetAllRecordsWithBetweenOperator()
    {
        $this->seed(['description' => ['N' => 10]]);
        $this->seed(['description' => ['N' => 11]]);

        $items = $this->testModel->where('description', 'BETWEEN', [1, 11])->get()->toArray();

        $this->assertEquals(2, count($items));

        $items = $this->testModel->where('description', 'BETWEEN', [100, 110])->get()->toArray();

        $this->assertEquals(0, count($items));
    }

    public function testGetFirstRecord()
    {
        $firstItem = $this->seed();

        $items = $this->testModel->first();

        $this->assertEquals(array_get($firstItem, 'id.S'), $items->id);
    }

    public function testChainedMethods()
    {
        $firstItem = $this->seed([
            'name' => ['S' => 'Same Name'],
            'description' => ['S' => 'First Description'],
        ]);

        $secondItem = $this->seed([
            'name' => ['S' => 'Same Name'],
            'description' => ['S' => 'Second Description'],
        ]);

        $foundItems = $this->testModel
            ->where('name', $firstItem['name']['S'])
            ->where('description', $firstItem['description']['S'])
            ->all();

        $this->assertEquals(1, $foundItems->count());

        $this->assertEquals($this->testModel->unmarshalItem($firstItem), $foundItems->first()->toArray());

        $foundItems = $this->testModel
            ->where('name', $secondItem['name']['S'])
            ->where('description', $secondItem['description']['S'])
            ->all();

        $this->assertEquals(1, $foundItems->count());

        $this->assertEquals($this->testModel->unmarshalItem($secondItem), $foundItems->first()->toArray());
    }

    public function testLookingUpByKey()
    {
        $this->seed();

        $item = $this->seed();

        $foundItems = $this->testModel->where('id', $item['id']['S'])->get();

        $this->assertEquals(1, $foundItems->count());

        $this->assertEquals($this->testModel->unmarshalItem($item), $foundItems->first()->toArray());
    }

    public function testCount()
    {
        $this->seed();
        $this->seed(['name' => ['S' => 'Foo']]);
        $this->seed(['name' => ['S' => 'Foo']]);

        $this->assertEquals(3, $this->testModel->count());
        $this->assertEquals(2, $this->testModel->where('name', 'Foo')->count());
    }

    public function testDifferentQueries()
    {
        $expectedFoo = $this->seed([
            'name' => ['S' => 'Foo'],
        ]);

        $expectedBar = $this->seed([
            'name' => ['S' => 'Bar'],
        ]);

        $fooQuery = $this->testModel->where('name', 'Foo');
        $barQuery = $this->testModel->where('name', 'Bar');

        $this->assertEquals(1, $fooQuery->count());
        $this->assertEquals(1, $fooQuery->count());
        $this->assertEquals($this->testModel->unmarshalItem($expectedFoo), $fooQuery->first()->toArray());
        $this->assertEquals($this->testModel->unmarshalItem($expectedBar), $barQuery->first()->toArray());
    }

    public function testChunkScan()
    {
        $this->seed(['name' => ['S' => 'Foo']]);
        $this->seed(['name' => ['S' => 'Foo2']]);
        $this->seed(['name' => ['S' => 'Foo3']]);

        $iteration = 1;

        $this->testModel->chunk(2, function ($results) use (&$iteration) {
            if ($iteration == 1) {
                $this->assertEquals(2, count($results));
            } else if ($iteration == 2) {
                $this->assertEquals(1, count($results));
            }

            $iteration++;
        });

        $this->assertEquals(3, $iteration);
    }

    public function testChunkScanCondition()
    {
        $this->seed(['name' => ['S' => 'Foo'], 'skey' => ['S' => 'test']]);
        $this->seed(['name' => ['S' => 'Foo1'], 'skey' => ['S' => 'test']]);
        $this->seed(['name' => ['S' => 'Foo2'], 'skey' => ['S' => 'test']]);
        $this->seed(['name' => ['S' => 'Foo3'], 'skey' => ['S' => 'test2']]);
        $this->seed(['name' => ['S' => 'Foo4'], 'skey' => ['S' => 'test2']]);

        $total_results = 0;

        // Because of how Scan works with conditions you can't guarantee each chunk size will === 2
        $this->testModel->where('skey', 'test')->chunk(2, function ($results) use (&$total_results) {
            $this->assertLessThanOrEqual(2, count($results));

            foreach ($results as $res) {
                $this->assertEquals('test', $res['skey']);
                $total_results++;
            }
        });

        $this->assertEquals(3, $total_results);
    }

    public function testStaticMethods()
    {
        $item = $this->seed(['name' => ['S' => 'Foo'], 'description' => ['S' => 'Bar']]);

        $item = $this->testModel->unmarshalItem($item);

        $this->assertEquals([$item], TestModel::all()->toArray());

        $this->assertEquals(1, TestModel::where('name', 'Foo')->where('description', 'Bar')->get()->count());

        $this->assertEquals($item, TestModel::first()->toArray());

        $this->assertEquals($item, TestModel::find($item['id'])->toArray());
    }

    public function testConditionContainingIndexKeyAndNonIndexKey()
    {
        $fooItem = $this->seed([
            'name' => ['S' => 'Foo'],
            'count' => ['N' => 10],
        ]);

        $barItem = $this->seed([
            'name' => ['S' => 'Bar'],
            'count' => ['N' => 11],
        ]);

        $expectedItem = $this->testModel->unmarshalItem($fooItem);

        $foundItems = $this->testModel
            ->where('count', 10)
            ->where('name', 'Foo')
            ->get();

        $this->assertEquals(1, $foundItems->count());
        $this->assertEquals($expectedItem, $foundItems->first()->toArray());
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

class TestModel extends \BaoPham\DynamoDb\DynamoDbModel
{
    protected $fillable = ['name', 'description', 'count'];

    protected $table = 'test_model';

    protected $dynamoDbIndexKeys = [
        'count_index' => [
            'hash' => 'count',
        ],
    ];
}
