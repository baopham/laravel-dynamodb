<?php

namespace BaoPham\DynamoDb\Tests;

/**
 * Class DynamoDbCompositeModelTest
 *
 * @package BaoPham\DynamoDb\Tests
 */
class DynamoDbCompositeModelTest extends DynamoDbModelTest
{
    protected function getTestModel()
    {
        return new CompositeTestModel([], $this->dynamoDb);
    }

    public function testCreateRecord()
    {
        $this->testModel->id = 'id1';
        $this->testModel->id2 = str_random(36);
        $this->testModel->name = 'Test Create';
        $this->testModel->count = 1;
        $this->testModel->save();

        $query = [
            'TableName' => $this->testModel->getTable(),
            'Key' => [
                'id' => ['S' => $this->testModel->id],
                'id2' => ['S' => $this->testModel->id2],
            ],
        ];

        $record = $this->dynamoDbClient->getItem($query)->toArray();

        $this->assertArrayHasKey('Item', $record);
        $this->assertEquals($this->testModel->id, $record['Item']['id']['S']);
        $this->assertEquals($this->testModel->id2, $record['Item']['id2']['S']);
    }

    public function testFindRecord()
    {
        $seed = $this->seed();
        $seedId = array_get($seed, 'id.S');
        $seedId2 = array_get($seed, 'id2.S');
        $seedName = array_get($seed, 'name.S');

        $item = $this->testModel->find(['id' => $seedId, 'id2' => $seedId2]);

        $this->assertNotEmpty($item);
        $this->assertEquals($seedId, $item->id);
        $this->assertEquals($seedId2, $item->id2);
        $this->assertEquals($seedName, $item->name);
    }

    public function testUpdateRecord()
    {
        $seed = $this->seed();
        $seedId = array_get($seed, 'id.S');
        $seedId2 = array_get($seed, 'id2.S');

        $newName = 'New Name';
        $this->testModel = $this->testModel->find(['id' => $seedId, 'id2' => $seedId2]);
        $updated = $this->testModel->update(['name' => $newName]);

        $this->assertTrue($updated);

        $query = [
            'TableName' => $this->testModel->getTable(),
            'Key' => [
                'id' => ['S' => $seedId],
                'id2' => ['S' => $seedId2],
            ],
        ];

        $record = $this->dynamoDbClient->getItem($query)->toArray();

        $this->assertEquals($newName, array_get($record, 'Item.name.S'));
    }

    public function testSaveRecord()
    {
        $seed = $this->seed();
        $seedId = array_get($seed, 'id.S');
        $seedId2 = array_get($seed, 'id2.S');

        $newName = 'New Name to be saved';
        $this->testModel = $this->testModel->find(['id' => $seedId, 'id2' => $seedId2]);
        $this->testModel->name = $newName;

        $this->testModel->save();

        $query = [
            'TableName' => $this->testModel->getTable(),
            'Key' => [
                'id' => ['S' => $seedId],
                'id2' => ['S' => $seedId2]
            ],
        ];

        $record = $this->dynamoDbClient->getItem($query)->toArray();

        $this->assertEquals($newName, array_get($record, 'Item.name.S'));
    }

    public function testDeleteRecord()
    {
        $seed = $this->seed();
        $seedId = array_get($seed, 'id.S');
        $seedId2 = array_get($seed, 'id2.S');

        $this->testModel->find(['id' => $seedId, 'id2' => $seedId2])->delete();

        $query = [
            'TableName' => $this->testModel->getTable(),
            'Key' => [
                'id' => ['S' => $seedId],
                'id2' => ['S' => $seedId2]
            ],
        ];

        $record = $this->dynamoDbClient->getItem($query)->toArray();

        $this->assertArrayNotHasKey('Item', $record);
    }

    public function testLookingUpByKey()
    {
        $this->seed();

        $item = $this->seed();

        $foundItems = $this->testModel
            ->where('id', $item['id']['S'])
            ->where('id2', $item['id2']['S'])
            ->get();

        $this->assertEquals(1, $foundItems->count());

        $this->assertEquals($this->testModel->unmarshalItem($item), $foundItems->first()->toArray());
    }

    public function testStaticMethods()
    {
        $item = $this->seed(['name' => ['S' => 'Foo'], 'description' => ['S' => 'Bar']]);

        $item = $this->testModel->unmarshalItem($item);

        $this->assertEquals([$item], CompositeTestModel::all()->toArray());

        $this->assertEquals(1, CompositeTestModel::where('name', 'Foo')->where('description', 'Bar')->get()->count());

        $this->assertEquals($item, CompositeTestModel::first()->toArray());

        $this->assertEquals($item, CompositeTestModel::find([
            'id' => $item['id'],
            'id2' => $item['id2']
        ])->toArray());
    }

    public function testConditionContainingCompositeIndexKey()
    {
        $fooItem = $this->seed([
            'id' => ['S' => 'id1'],
            'id2' => ['S' => '2'],
            'name' => ['S' => 'Foo'],
            'count' => ['N' => 11],
        ]);

        $barItem = $this->seed([
            'id' => ['S' => 'id1'],
            'id2' => ['S' => '1'],
            'name' => ['S' => 'Bar'],
            'count' => ['N' => 9],
        ]);

        $bazItem = $this->seed([
            'id' => ['S' => 'id1'],
            'id2' => ['S' => '3'],
            'name' => ['S' => 'Baz'],
            'count' => ['N' => 10],
        ]);

        // Test condition contains all composite keys with valid operator
        $foundItems = $this->testModel
            ->where('id', 'id1')
            ->where('count', '>=', 10) // Test range key support comparison operator other than EQ
            ->get();

        // If id_count_index is used, $bazItem must be the first found item
        $expectedItem = $this->testModel->unmarshalItem($bazItem);

        $this->assertEquals(2, $foundItems->count());
        $this->assertEquals($expectedItem, $foundItems->first()->toArray());

        // Test condition contains all composite keys with invalid operator
        $foundItems = $this->testModel
            ->where('id', 'begins_with', 'id') // Invalid operator for hash key
            ->where('count', '>', 0)
            ->get();

        // id_count_index is not used because of invalid operator for hash key
        // A normal Scan operation is used, results are sorted by id2
        $expectedItem = $this->testModel->unmarshalItem($barItem);

        $this->assertEquals(3, $foundItems->count());
        $this->assertEquals($expectedItem, $foundItems->first()->toArray());
    }

    public function testConditionsDoNotContainAllCompositeKeys()
    {
        $fooItem = $this->seed([
            'id' => ['S' => 'id1'],
            'id2' => ['S' => '2'],
            'name' => ['S' => 'Foo'],
            'count' => ['N' => 1],
        ]);

        $barItem = $this->seed([
            'id' => ['S' => 'id1'],
            'id2' => ['S' => '1'],
            'name' => ['S' => 'Bar'],
            'count' => ['N' => 2],
        ]);

        $foundItems = $this->testModel
            ->where('count', '>', 0)
            ->get();

        // id_count_index is not used because conditions don't have all composite keys
        // A normal Scan operation is used, results are sorted by id2
        $expectedItem = $this->testModel->unmarshalItem($barItem);

        $this->assertEquals(2, $foundItems->count());
        $this->assertEquals($expectedItem, $foundItems->first()->toArray());
    }

    protected function seed($attributes = [])
    {
        $item = [
            'id' => ['S' => 'id1'],
            'id2' => ['S' => str_random(36)],
            'name' => ['S' => str_random(36)],
            'description' => ['S' => str_random(256)],
            'count' => ['N' => rand()],
        ];

        $item = array_merge($item, $attributes);

        $this->dynamoDbClient->putItem([
            'TableName' => $this->testModel->getTable(),
            'Item' => $item,
        ]);

        return $item;
    }

}

class CompositeTestModel extends \BaoPham\DynamoDb\DynamoDbModel
{
    protected $fillable = ['name', 'description', 'count'];

    protected $table = 'composite_test_model';

    protected $compositeKey = ['id', 'id2'];

    protected $dynamoDbIndexKeys = [
        'id_count_index' => [
            'hash' => 'id',
            'range' => 'count',
        ],
    ];
}
