<?php

use Aws\DynamoDb\Marshaler;
use BaoPham\DynamoDb\DynamoDbClientService;
use BaoPham\DynamoDb\EmptyAttributeFilter;

class DynamoDbModelTest extends TestCase
{
    /**
     * @var \Aws\DynamoDb\DynamoDbClient
     */
    protected $dynamoDbClient;

    /**
     * @var \BaoPham\DynamoDb\DynamoDbClientService
     */
    protected $dynamoDb;

    /**
     * @var TestModel
     */
    protected $testModel;

    public function setUp()
    {
        parent::setUp();

        $this->setUpDatabase();

        $this->bindDynamoDbClientInstance();
    }

    protected function bindDynamoDbClientInstance()
    {
        $marshalerOptions = [
            'nullify_invalid' => true,
        ];

        $config = [
            'credentials' => [
                'key' => 'dynamodb_local',
                'secret' => 'secret',
            ],
            'region' => 'test',
            'version' => '2012-08-10',
            'endpoint' => 'http://localhost:3000',
        ];
        $this->dynamoDb = new DynamoDbClientService($config, new Marshaler($marshalerOptions),
            new EmptyAttributeFilter);

        $this->testModel = new TestModel([], $this->dynamoDb);

        $this->dynamoDbClient = $this->dynamoDb->getClient();

    }

    protected function setUpDatabase()
    {
        copy(dirname(__FILE__) . '/../dynamodb_local_init.db', dirname(__FILE__) . '/../dynamodb_local_test.db');
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
        $this->assertEquals("1", $record['Item']['version']['N']);
        $this->assertEquals($record['Item']['created_at']['S'], $record['Item']['updated_at']['S']);
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

        //Introduce a 1 second delay to confirm updated_at time is set correctly
        //updated_at must be 1 second later than created_at
        sleep(1);

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
        $this->assertEquals("2", array_get($record, 'Item.version.N'));
        $this->assertNotEquals(array_get($record, 'Item.created_at.S'), array_get($record, 'Item.updated_at.S'));
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

    public function testOptimisticLocking()
    {
        $firstItem = $this->seed();
        $item1 = $this->testModel->first();
        $item2 = $this->testModel->first();

        $updated = $item1->update([name => 'Updated First']);
        $this->assertTrue($updated);

        try {
            $updated = $item2->update([name => 'Updated Second']);
            $this->fail('Conflicting update has succeeded but it should fail.');
        } catch (Exception $e) {
            //This is correct, an exception is expected because the second update is conflicting
        }

        $item2 = $this->testModel->first();
        $updated = $item2->update([name => 'Updated Second']);
        $this->assertTrue($updated);

    }

    protected function seed($attributes = [])
    {
        $item = [
            'id' => ['S' => str_random(36)],
            'name' => ['S' => str_random(36)],
            'description' => ['S' => str_random(256)],
            'count' => ['N' => rand()],
            'version' => ['N' => '1'],
            'created_at' => ['S' => date("Y-m-d H:i:s")],
            'updated_at' => ['S' => date("Y-m-d H:i:s")],
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
