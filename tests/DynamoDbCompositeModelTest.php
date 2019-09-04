<?php

namespace BaoPham\DynamoDb\Tests;

use BaoPham\DynamoDb\DynamoDbModel;
use BaoPham\DynamoDb\Facades\DynamoDb;
use BaoPham\DynamoDb\RawDynamoDbQuery;
use \Illuminate\Database\Eloquent\ModelNotFoundException;
use Illuminate\Support\Arr;
use Illuminate\Support\Str;

/**
 * Class DynamoDbCompositeModelTest
 *
 * @package BaoPham\DynamoDb\Tests
 */
class DynamoDbCompositeModelTest extends DynamoDbNonCompositeModelTest
{
    protected function getTestModel()
    {
        return new CompositeKeyWithIndex();
    }

    public function testCreateRecord()
    {
        $this->testModel->id = 'id1';
        $this->testModel->id2 = Str::random(36);
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

        $record = $this->getClient()->getItem($query)->toArray();

        $this->assertArrayHasKey('Item', $record);
        $this->assertEquals($this->testModel->id, $record['Item']['id']['S']);
        $this->assertEquals($this->testModel->id2, $record['Item']['id2']['S']);
    }

    public function testCreateAsyncRecord()
    {
        $this->testModel->id = 'id1';
        $this->testModel->id2 = Str::random(36);
        $this->testModel->name = 'Test Create Async';
        $this->testModel->count = 1;
        $this->testModel->saveAsync()->wait();

        $query = [
            'TableName' => $this->testModel->getTable(),
            'Key' => [
                'id' => ['S' => $this->testModel->id],
                'id2' => ['S' => $this->testModel->id2],
            ],
        ];

        $record = $this->getClient()->getItem($query)->toArray();

        $this->assertArrayHasKey('Item', $record);
        $this->assertEquals($this->testModel->id, $record['Item']['id']['S']);
        $this->assertEquals($this->testModel->id2, $record['Item']['id2']['S']);
    }

    public function testFindRecord()
    {
        $seed = $this->seed();
        $seedId = Arr::get($seed, 'id.S');
        $seedId2 = Arr::get($seed, 'id2.S');
        $seedName = Arr::get($seed, 'name.S');

        $item = $this->testModel->find(['id' => $seedId, 'id2' => $seedId2]);

        $this->assertNotEmpty($item);
        $this->assertEquals($seedId, $item->id);
        $this->assertEquals($seedId2, $item->id2);
        $this->assertEquals($seedName, $item->name);
    }

    public function testFindMultiple()
    {
        $hash = ['foo', 'foo1'];
        $range = ['bar', 'bar2'];
        $ids = [
            ['id' => $hash[0], 'id2' => $range[0]],
            ['id' => $hash[1], 'id2' => $range[1]],
        ];
        $this->seed(DynamoDb::marshalItem($ids[0]));
        $this->seed(DynamoDb::marshalItem($ids[1]));

        $assert = function ($results) use ($hash, $range) {
            $this->assertCount(2, $results);
            $this->assertContains($results->first()->id, $hash);
            $this->assertContains($results->first()->id2, $range);
            $this->assertContains($results->last()->id, $hash);
            $this->assertContains($results->last()->id2, $range);
        };

        $results = $this->testModel->find($ids);

        $assert($results);

        $results = $this->testModel->findMany($ids);

        $assert($results);
    }

    public function testFindOrFailRecordPass()
    {
        $seed = $this->seed();
        $seedId = Arr::get($seed, 'id.S');
        $seedId2 = Arr::get($seed, 'id2.S');
        $seedName = Arr::get($seed, 'name.S');

        $item = $this->testModel->findOrFail(['id' => $seedId, 'id2' => $seedId2]);

        $this->assertNotEmpty($item);
        $this->assertEquals($seedId, $item->id);
        $this->assertEquals($seedId2, $item->id2);
        $this->assertEquals($seedName, $item->name);
    }

    public function testFindOrFailMultiple()
    {
        $hash = ['foo', 'foo1'];
        $range = ['bar', 'bar2'];
        $ids = [
            ['id' => $hash[0], 'id2' => $range[0]],
            ['id' => $hash[1], 'id2' => $range[1]],
        ];
        $this->seed(DynamoDb::marshalItem($ids[0]));
        $this->seed(DynamoDb::marshalItem($ids[1]));

        $results = $this->testModel->find($ids);

        $this->assertCount(2, $results);
        $this->assertContains($results->first()->id, $hash);
        $this->assertContains($results->first()->id2, $range);
        $this->assertContains($results->last()->id, $hash);
        $this->assertContains($results->last()->id2, $range);
    }

    public function testFirstOrFailRecordPass()
    {
        $seed = $this->seed();
        $seedId = Arr::get($seed, 'id.S');
        $seedId2 = Arr::get($seed, 'id2.S');
        $seedName = Arr::get($seed, 'name.S');

        $query = $this->testModel
            ->where('id', $seedId)
            ->where('id2', $seedId2);

        $first = $query->firstOrFail();
        $this->assertNotEmpty($first);
        $this->assertEquals($seedId, $first->id);
        $this->assertEquals($seedId2, $first->id2);
        $this->assertEquals($seedName, $first->name);
        $this->assertEquals('Query', $query->toDynamoDbQuery()->op);
    }

    public function testFindOrFailRecordFail()
    {
        $this->expectException(ModelNotFoundException::class);
        $this->testModel->findOrFail(['id' => 'failure-expected', 'id2' => 'expected-to-fail']);
    }

    public function testFirstOrFailRecordFail()
    {
        $this->expectException(ModelNotFoundException::class);
        $this->testModel
            ->where('id', 'failure-expected')
            ->where('id2', 'expected-to-fail')
            ->firstOrFail();
    }

    public function testUpdateRecord()
    {
        $seed = $this->seed();
        $seedId = Arr::get($seed, 'id.S');
        $seedId2 = Arr::get($seed, 'id2.S');

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

        $record = $this->getClient()->getItem($query)->toArray();

        $this->assertEquals($newName, Arr::get($record, 'Item.name.S'));
    }

    public function testUpdateAsyncRecord()
    {
        $seed = $this->seed();
        $seedId = Arr::get($seed, 'id.S');
        $seedId2 = Arr::get($seed, 'id2.S');

        $newName = 'New Name';
        $this->testModel = $this->testModel->find(['id' => $seedId, 'id2' => $seedId2]);
        $this->testModel->updateAsync(['name' => $newName])->wait();

        $query = [
            'TableName' => $this->testModel->getTable(),
            'Key' => [
                'id' => ['S' => $seedId],
                'id2' => ['S' => $seedId2],
            ],
        ];

        $record = $this->getClient()->getItem($query)->toArray();

        $this->assertEquals($newName, Arr::get($record, 'Item.name.S'));
    }

    public function testSaveRecord()
    {
        $seed = $this->seed();
        $seedId = Arr::get($seed, 'id.S');
        $seedId2 = Arr::get($seed, 'id2.S');

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

        $record = $this->getClient()->getItem($query)->toArray();

        $this->assertEquals($newName, Arr::get($record, 'Item.name.S'));
    }

    public function testSaveAsyncRecord()
    {
        $seed = $this->seed();
        $seedId = Arr::get($seed, 'id.S');
        $seedId2 = Arr::get($seed, 'id2.S');

        $newName = 'New Name to be saved asynchronously';
        $this->testModel = $this->testModel->find(['id' => $seedId, 'id2' => $seedId2]);
        $this->testModel->name = $newName;

        $this->testModel->saveAsync()->wait();

        $query = [
            'TableName' => $this->testModel->getTable(),
            'Key' => [
                'id' => ['S' => $seedId],
                'id2' => ['S' => $seedId2]
            ],
        ];

        $record = $this->getClient()->getItem($query)->toArray();

        $this->assertEquals($newName, Arr::get($record, 'Item.name.S'));
    }

    public function testDeleteRecord()
    {
        $seed = $this->seed();
        $seedId = Arr::get($seed, 'id.S');
        $seedId2 = Arr::get($seed, 'id2.S');

        $this->testModel->find(['id' => $seedId, 'id2' => $seedId2])->delete();

        $query = [
            'TableName' => $this->testModel->getTable(),
            'Key' => [
                'id' => ['S' => $seedId],
                'id2' => ['S' => $seedId2]
            ],
        ];

        $record = $this->getClient()->getItem($query)->toArray();

        $this->assertArrayNotHasKey('Item', $record);
    }

    public function testDeleteAsyncRecord()
    {
        $seed = $this->seed();
        $seedId = Arr::get($seed, 'id.S');
        $seedId2 = Arr::get($seed, 'id2.S');

        $this->testModel->find(['id' => $seedId, 'id2' => $seedId2])->deleteAsync()->wait();

        $query = [
            'TableName' => $this->testModel->getTable(),
            'Key' => [
                'id' => ['S' => $seedId],
                'id2' => ['S' => $seedId2]
            ],
        ];

        $record = $this->getClient()->getItem($query)->toArray();

        $this->assertArrayNotHasKey('Item', $record);
    }

    public function testLookUpByKey()
    {
        $this->seed();

        $item = $this->seed();

        $query = $this->testModel
            ->where('id', $item['id']['S'])
            ->where('id2', $item['id2']['S']);

        $this->assertEquals('Query', $query->toDynamoDbQuery()->op);

        $foundItems = $query->get();
        $this->assertEquals(1, $foundItems->count());
        $this->assertEquals($this->marshaler->unmarshalItem($item), $foundItems->first()->toArray());
    }

    public function testSearchByHashAndSortKey()
    {
        $partitionKey = 'foo';
        $item1 = $this->seed([
            'id' => ['S' => $partitionKey],
            'id2' => ['S' => 'bar_1']
        ]);
        $item2 = $this->seed([
            'id' => ['S' => $partitionKey],
            'id2' => ['S' => 'bar_2']
        ]);
        $this->seed([
            'id' => ['S' => 'other'],
            'id2' => ['S' => 'foo_1']
        ]);

        $query = $this->testModel
            ->where('id', $partitionKey)
            ->where('id2', 'begins_with', 'bar');

        $dynamoDbQuery = $query->toDynamoDbQuery();
        $this->assertEquals('Query', $dynamoDbQuery->op);
        $this->assertArrayNotHasKey('FilterExpression', $dynamoDbQuery->query);

        $foundItems = $query->get();
        $this->assertEquals(2, $foundItems->count());
        $this->assertEquals($this->marshaler->unmarshalItem($item1), $foundItems->first()->toArray());
        $this->assertEquals($this->marshaler->unmarshalItem($item2), $foundItems->last()->toArray());
    }

    public function testStaticMethods()
    {
        $item = $this->seed(['name' => ['S' => 'Foo'], 'description' => ['S' => 'Bar']]);

        $item = $this->marshaler->unmarshalItem($item);

        $klass = get_class($this->testModel);

        $this->assertEquals([$item], $klass::all()->toArray());

        $this->assertEquals(1, $klass::where('name', 'Foo')->where('description', 'Bar')->get()->count());

        $this->assertEquals($item, $klass::first()->toArray());

        $this->assertEquals($item, $klass::find([
            'id' => $item['id'],
            'id2' => $item['id2']
        ])->toArray());
    }

    public function testConditionContainingIndexKey()
    {
        $this->seed([
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
        $query = $this->testModel
            ->where('id', 'id1')
            // Test range key support comparison operator other than EQ
            ->where('count', '>=', 10);

        $dynamoDbQuery = $query->toDynamoDbQuery();
        $this->assertEquals('Query', $dynamoDbQuery->op);
        $this->assertEquals('#id = :a1 AND #count >= :a2', $dynamoDbQuery->query['KeyConditionExpression']);

        $foundItems = $query->get();
        $this->assertEquals(2, $foundItems->count());

        // If id_count_index is used, $bazItem must be the first found item
        $expectedItem = $this->marshaler->unmarshalItem($bazItem);
        $this->assertEquals($expectedItem, $foundItems->first()->toArray());

        // Test condition contains all composite keys with invalid operator
        $query = $this->testModel
            // Invalid operator for hash key
            ->where('id', 'begins_with', 'id')
            ->where('count', '>', 0);

        $dynamoDbQuery = $query->toDynamoDbQuery();
        $this->assertEquals('Scan', $dynamoDbQuery->op);
        $this->assertEquals('begins_with(#id, :a1) AND #count > :a2', $dynamoDbQuery->query['FilterExpression']);

        $foundItems = $query->get();
        $this->assertEquals(3, $foundItems->count());

        // id_count_index is not used because of invalid operator for hash key
        // A normal Scan operation is used, results are sorted by id2
        $expectedItem = $this->marshaler->unmarshalItem($barItem);
        $this->assertEquals($expectedItem, $foundItems->first()->toArray());
    }

    public function testSetIndexManually()
    {
        $raw = $this->testModel
            ->where('id', 'id1')
            ->where('author', 'BP')
            ->withIndex('id_author_index')
            ->toDynamoDbQuery();

        $this->assertEquals('id_author_index', Arr::get($raw->query, 'IndexName'));
        $this->assertEquals('Query', $raw->op);
    }

    public function testConditionsNotContainingAllCompositeKeys()
    {
        $this->seed([
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
        $expectedItem = $this->marshaler->unmarshalItem($barItem);

        $this->assertEquals(2, $foundItems->count());
        $this->assertEquals($expectedItem, $foundItems->first()->toArray());
    }

    public function testRemoveNestedAttribute()
    {
        $this->seed([
            'id' => ['S' => 'foo'],
            'id2' => ['S' => 'bar']
        ]);

        $this->testModel
            ->where('id', 'foo')
            ->where('id2', 'bar')
            ->removeAttribute('nested.foo');

        $this->assertArrayNotHasKey('foo', $this->testModel->nested);
        $this->assertFalse($this->testModel->isDirty());

        $item = $this->testModel->find(['id' => 'foo', 'id2' => 'bar']);
        $this->assertArrayNotHasKey('foo', $item->nested);
    }

    public function testRemoveAttributesOnQuery()
    {
        $this->seed([
            'id' => ['S' => 'foo'],
            'id2' => ['S' => 'bar']
        ]);

        $this->testModel
            ->where('id', 'foo')
            ->where('id2', 'bar')
            ->removeAttribute('description', 'name', 'nested.foo', 'nested.nestedArray[0]', 'nestedArray[0]');

        $this->assertRemoveAttributes($this->testModel);
        $this->assertFalse($this->testModel->isDirty());

        $item = $this->testModel->find(['id' => 'foo', 'id2' => 'bar']);
        $this->assertRemoveAttributes($item);
    }

    public function testRemoveAttributesOnModel()
    {
        $this->seed([
            'id' => ['S' => 'foo'],
            'id2' => ['S' => 'bar']
        ]);

        $item = $this->testModel->first();
        $item->removeAttribute('description', 'name', 'nested.foo', 'nested.nestedArray[0]', 'nestedArray[0]');

        $this->assertRemoveAttributes($item);
        $this->assertFalse($item->isDirty());

        $item = $this->testModel->first();
        $this->assertRemoveAttributes($item);
    }

    public function testAfterForQueryOperation()
    {
        foreach (range(0, 9) as $i) {
            $this->seed(['count' => ['N' => $i]]);
        }

        // Paginate 2 items at a time
        $pageSize = 2;
        $last = null;
        $paginationResult = collect();

        do {
            $items = $this->testModel
                ->where('count', '>', -1)
                ->after($last)
                ->limit($pageSize)->all();
            $last = $items->last();
            $paginationResult = $paginationResult->merge($items->pluck('count'));
        } while ($last);

        $this->assertEquals(range(0, 9), $paginationResult->sort()->values()->toArray());
    }

    public function testAfterKeyForQueryOperation()
    {
        for ($i = 0; $i < 10; $i++) {
            $this->seed(['id' => ['S' => 'id'], 'id2' => ['S' => "$i"]]);
        }

        $assert = function (callable $getKey) {
            $paginationResult = collect();
            $afterKey = null;
            $query = $this->testModel->where('id', 'id')->where('id2', '>', '-1');

            $this->assertEquals('Query', $query->toDynamoDbQuery()->op);

            do {
                $items = $query->afterKey($afterKey)->limit(2)->all();
                $paginationResult = $paginationResult->merge($items->pluck('id2'));
                $afterKey = $getKey($items);
            } while ($afterKey);

            $this->assertCount(10, $paginationResult);
            $paginationResult->each(function ($id) {
                $this->assertGreaterThan('-1', $id);
            });
        };

        $assert(function ($items) {
            return $items->lastKey();
        });

        $assert(function ($items) {
            return !$items->isEmpty() ? $items->last()->getKeys() : null;
        });
    }

    public function testDecorateRawQuery()
    {
        foreach (range(0, 9) as $i) {
            $this->seed(['id' => ['S' => 'id'], 'count' => ['N' => $i]]);
        }

        $query = $this->testModel
            ->where('id', 'id')
            ->where('count', '>=', 0);

        $forward = $query->get();

        $this->assertEquals(range(0, 9), $forward->pluck('count')->toArray());

        $query->decorate(function (RawDynamoDbQuery $raw) {
            $raw->query['ScanIndexForward'] = false;
        });

        $reverse = $query->get();

        $this->assertEquals(range(9, 0, -1), $reverse->pluck('count')->toArray());
    }

    public function testUsingBothKeyAndFilterConditionsForModelWithoutIndex()
    {
        foreach (range(0, 9) as $i) {
            $this->seed([
                'id' => ['S' => 'id'],
                'id2' => ['S' => "$i"],
                'count' => ['N' => $i],
            ]);
        }

        $query = with(new CompositeKeyWithoutIndex())
            ->where('id', 'id')
            ->where('id2', '>', '4')
            ->where('count', '<=', 8);

        $dynamoDbQuery = $query->toDynamoDbQuery();

        $this->assertEquals('Query', $dynamoDbQuery->op);

        $this->assertEquals(
            '#id = :a1 AND #id2 > :a2',
            $dynamoDbQuery->query['KeyConditionExpression']
        );

        $this->assertEquals(
            '#count <= :a3',
            $dynamoDbQuery->query['FilterExpression']
        );

        $result = $query->get();
        $expected = range(5, 8);

        $this->assertEquals(
            $expected,
            $result->pluck('count')->toArray()
        );

        $this->assertEquals(
            array_map('strval', $expected),
            $result->pluck('id2')->toArray()
        );
    }

    public function testUsingBothKeyAndFilterConditionsForModelWithIndex()
    {
        foreach (range(0, 9) as $i) {
            $this->seed([
                'id' => ['S' => 'id'],
                'id2' => ['S' => "$i"],
                'count' => ['N' => $i],
            ]);
        }

        $query = with(new CompositeKeyWithIndex())
            ->where('id', 'id')
            ->where('id2', '>', '4')
            ->where('count', '<=', 8);

        $dynamoDbQuery = $query->toDynamoDbQuery();

        $this->assertEquals('Query', $dynamoDbQuery->op);

        $this->assertEquals(
            '#id = :a1 AND #count <= :a2',
            $dynamoDbQuery->query['KeyConditionExpression']
        );

        $this->assertEquals(
            '#id2 > :a3',
            $dynamoDbQuery->query['FilterExpression']
        );

        $result = $query->get();
        $expected = range(5, 8);

        $this->assertEquals(
            $expected,
            $result->pluck('count')->toArray()
        );

        $this->assertEquals(
            array_map('strval', $expected),
            $result->pluck('id2')->toArray()
        );
    }

    public function testRefresh()
    {
        $this->seed();

        $model = $this->testModel->first();

        $originalHash = $model->id;
        $originalRange = $model->id2;
        $originalName = $model->name;

        $model->name = 'Modified Name';

        $refreshed = $model->refresh();

        $this->assertEquals($originalName, $model->name);
        $this->assertEquals($originalHash, $model->id);
        $this->assertEquals($originalRange, $model->id2);
        $this->assertEquals($refreshed, $model);
    }

    public function testFindWithColumns()
    {
        $this->seed([
            'id' => ['S' => 'foo'],
            'id2' => ['S' => 'bar'],
            'name' => ['S' => 'baz'],
            'nested' => [
                'M' => [
                    'key1' => ['S' => 'value1'],
                    'key2' => ['S' => 'value2'],
                ],
            ],
        ]);

        $columns = ['id', 'name', 'nested.key1'];

        $expected = [
            'id' => 'foo',
            'name' => 'baz',
            'nested' => ['key1' => 'value1'],
        ];

        $result = $this->testModel
            ->find(['id' => 'foo', 'id2' => 'bar'], $columns);

        $this->assertEquals($expected, $result->toArray());
    }

    public function seed($attributes = [], $exclude = [])
    {
        $item = [
            'id' => ['S' => 'id1'],
            'id2' => ['S' => Str::random(36)],
            'name' => ['S' => Str::random(36)],
            'description' => ['S' => Str::random(256)],
            'count' => ['N' => rand()],
            'author' => ['S' => Str::random()],
            'nested' => [
                'M' => [
                    'foo' => ['S' => 'bar'],
                    'nestedArray' => ['L' => [['S' => 'first']]],
                    'hello' => ['S' => 'world'],
                ],
            ],
            'nestedArray' => [
                'L' => [
                    ['S' => 'first'],
                    ['S' => 'second'],
                ],
            ],
        ];

        $item = array_merge($item, $attributes);
        $item = Arr::except($item, $exclude);

        $this->getClient()->putItem([
            'TableName' => $this->testModel->getTable(),
            'Item' => $item,
        ]);

        return $item;
    }
}

// phpcs:disable PSR1.Classes.ClassDeclaration.MultipleClasses
class CompositeKeyWithIndex extends DynamoDbModel
{
    protected $fillable = [
        'name',
        'description',
        'count',
        'author',
        'nested',
        'nestedArray',
    ];

    protected $table = 'composite_test_model';

    protected $connection = 'test';

    protected $compositeKey = ['id', 'id2'];

    protected $dynamoDbIndexKeys = [
        'id_count_index' => [
            'hash' => 'id',
            'range' => 'count',
        ],
        // extra index for testing setting index manually, not yet provisioned
        'id_author_index' => [
            'hash' => 'id',
            'range' => 'author',
        ],
    ];
}

class CompositeKeyWithoutIndex extends DynamoDbModel
{
    protected $fillable = [
        'name',
        'description',
        'count',
        'author',
        'nested',
        'nestedArray',
    ];

    protected $table = 'composite_test_model';

    protected $connection = 'test';

    protected $compositeKey = ['id', 'id2'];
}
// phpcs:enable PSR1.Classes.ClassDeclaration.MultipleClasses
