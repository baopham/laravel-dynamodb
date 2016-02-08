<?php

class DynamoDbCompositeModelTest extends ModelTest
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

    public function testGetFirstRecord()
    {
        $firstItem = $this->seed();

        $items = $this->testModel->first();

        $this->assertEquals(array_get($firstItem, 'id.S'), $items->id);
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
        'count' => 'count_index',
    ];
}
