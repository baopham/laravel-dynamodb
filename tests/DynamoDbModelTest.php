<?php


class DynamoDbModelTest extends ModelTest
{
    protected function getTestModel()
    {
        return new TestModel([], $this->dynamoDb);
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

    public function testGetFirstRecord()
    {
        $firstItem = $this->seed();

        $items = $this->testModel->first();

        $this->assertEquals(array_get($firstItem, 'id.S'), $items->id);
    }

    protected function seed($attributes = [])
    {
        $item = [
            'id' => ['S' => str_random(36)],
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

class TestModel extends \BaoPham\DynamoDb\DynamoDbModel
{
    protected $fillable = ['name', 'description', 'count'];

    protected $table = 'test_model';

    protected $dynamoDbIndexKeys = [
        'count' => 'count_index',
    ];
}
