<?php

namespace BaoPham\DynamoDb\Tests;

use BaoPham\DynamoDb\DynamoDbModel;
use BaoPham\DynamoDb\RawDynamoDbQuery;
use \Illuminate\Database\Eloquent\ModelNotFoundException;
use Illuminate\Support\Arr;
use Illuminate\Support\Str;

/**
 * Class DynamoDbNonCompositeModelTest
 *
 * @package BaoPham\DynamoDb\Tests
 */
class DynamoDbNonCompositeModelTest extends DynamoDbModelTestBase
{
    protected function getTestModel()
    {
        return new PrimaryKeyWithIndexModel();
    }

    public function testCreateRecord()
    {
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
        $this->assertEquals($this->testModel->id, $record['Item']['id']['S']);
    }

    public function testCreateAsyncRecord()
    {
        $this->testModel->id = Str::random(36);
        $this->testModel->name = 'Test Create Async';
        $this->testModel->count = 1;
        $this->testModel->saveAsync()->wait();

        $query = [
            'TableName' => $this->testModel->getTable(),
            'Key' => [
                'id' => ['S' => $this->testModel->id]
            ],
        ];

        $record = $this->getClient()->getItem($query)->toArray();

        $this->assertArrayHasKey('Item', $record);
        $this->assertEquals($this->testModel->id, $record['Item']['id']['S']);
    }

    public function testFindRecord()
    {
        $seed = $this->seed();
        $seedId = Arr::get($seed, 'id.S');
        $seedName = Arr::get($seed, 'name.S');

        $item = $this->testModel->find($seedId);

        $this->assertNotEmpty($item);
        $this->assertEquals($seedId, $item->id);
        $this->assertEquals($seedName, $item->name);
    }

    public function testFindMultiple()
    {
        $ids = ['foo', 'bar'];
        $this->seed(['id' => ['S' => $ids[0]]]);
        $this->seed(['id' => ['S' => $ids[1]]]);

        $assert = function ($results) use ($ids) {
            $this->assertCount(2, $results);
            $this->assertContains($results->first()->id, $ids);
            $this->assertContains($results->last()->id, $ids);
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
        $seedName = Arr::get($seed, 'name.S');

        $item = $this->testModel->findOrFail($seedId);

        $this->assertNotEmpty($item);
        $this->assertEquals($seedId, $item->id);
        $this->assertEquals($seedName, $item->name);
    }

    public function testFindOrFailMultiple()
    {
        $ids = ['foo', 'bar'];
        $this->seed(['id' => ['S' => $ids[0]]]);
        $this->seed(['id' => ['S' => $ids[1]]]);

        $results = $this->testModel->findOrFail($ids);

        $this->assertCount(2, $results);
        $this->assertContains($results->first()->id, $ids);
        $this->assertContains($results->last()->id, $ids);
    }

    public function testFirstOrFailRecordPass()
    {
        $seed = $this->seed();
        $seedId = Arr::get($seed, 'id.S');
        $seedName = Arr::get($seed, 'name.S');

        $query = $this->testModel
            ->where('id', $seedId);

        $first = $query->firstOrFail();
        $this->assertNotEmpty($first);
        $this->assertEquals($seedId, $first->id);
        $this->assertEquals($seedName, $first->name);
        $this->assertEquals('Query', $query->toDynamoDbQuery()->op);
    }

    public function testFindOrFailRecordFail()
    {
        $this->expectException(ModelNotFoundException::class);
        $this->testModel->findOrFail('expected-to-fail');
    }

    public function testFirstOrFailRecordFail()
    {
        $this->expectException(ModelNotFoundException::class);
        $this->testModel
            ->where('id', 'expected-to-fail')
            ->firstOrFail();
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
        $seedId = Arr::get($seed, 'id.S');

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

        $record = $this->getClient()->getItem($query)->toArray();

        $this->assertEquals($newName, Arr::get($record, 'Item.name.S'));
    }

    public function testUpdateAsyncRecord()
    {
        $seed = $this->seed();
        $seedId = Arr::get($seed, 'id.S');

        $newName = 'New Name';
        $this->testModel = $this->testModel->find($seedId);
        $this->testModel->updateAsync(['name' => $newName])->wait();

        $query = [
            'TableName' => $this->testModel->getTable(),
            'Key' => [
                'id' => ['S' => $seedId]
            ],
        ];

        $record = $this->getClient()->getItem($query)->toArray();

        $this->assertEquals($newName, Arr::get($record, 'Item.name.S'));
    }

    public function testSaveRecord()
    {
        $seed = $this->seed();
        $seedId = Arr::get($seed, 'id.S');

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

        $record = $this->getClient()->getItem($query)->toArray();

        $this->assertEquals($newName, Arr::get($record, 'Item.name.S'));
    }

    public function testSaveAsyncRecord()
    {
        $seed = $this->seed();
        $seedId = Arr::get($seed, 'id.S');

        $newName = 'New Name to be saved asynchronously';
        $this->testModel = $this->testModel->find($seedId);
        $this->testModel->name = $newName;

        $this->testModel->saveAsync()->wait();

        $query = [
            'TableName' => $this->testModel->getTable(),
            'Key' => [
                'id' => ['S' => $seedId]
            ],
        ];

        $record = $this->getClient()->getItem($query)->toArray();

        $this->assertEquals($newName, Arr::get($record, 'Item.name.S'));
    }

    public function testDeleteRecord()
    {
        $seed = $this->seed();
        $seedId = Arr::get($seed, 'id.S');

        $this->testModel->find($seedId)->delete();

        $query = [
            'TableName' => $this->testModel->getTable(),
            'Key' => [
                'id' => ['S' => $seedId]
            ],
        ];

        $record = $this->getClient()->getItem($query)->toArray();

        $this->assertArrayNotHasKey('Item', $record);
    }

    public function testDeleteAsyncRecord()
    {
        $seed = $this->seed();
        $seedId = Arr::get($seed, 'id.S');

        $this->testModel->find($seedId)->deleteAsync()->wait();

        $query = [
            'TableName' => $this->testModel->getTable(),
            'Key' => [
                'id' => ['S' => $seedId]
            ],
        ];

        $record = $this->getClient()->getItem($query)->toArray();

        $this->assertArrayNotHasKey('Item', $record);
    }

    public function testGetAllRecordsWithEqualCondition()
    {
        $this->seed(['count' => ['N' => 10]]);
        $this->seed(['count' => ['N' => 10]]);
        $this->seed(['count' => ['N' => 10]]);
        $this->seed(['count' => ['N' => 9]]);

        $items = $this->testModel->where('count', 10)->get();

        $this->assertEquals(3, $items->count());

        // OR condition
        $items = $this->testModel
            ->where('count', 10)
            ->orWhere('count', 9)
            ->get();

        $this->assertEquals(4, $items->count());
    }

    public function testGetAllRecordsWithNonEqualCondition()
    {
        $this->seed(['count' => ['N' => 10]]);
        $this->seed(['count' => ['N' => 11]]);
        $this->seed(['count' => ['N' => 11]]);

        $items = $this->testModel->where('count', '!=', 10)->get();

        $this->assertEquals(2, $items->count());

        // OR condition
        $items = $this->testModel
            ->where('count', '!=', 10)
            ->orWhere('count', '!=', 11)
            ->get();

        $this->assertEquals(3, $items->count());
    }

    public function testGetAllRecordsWithGTCondition()
    {
        $this->seed(['count' => ['N' => 10]]);
        $this->seed(['count' => ['N' => 11]]);
        $this->seed(['count' => ['N' => 11]]);

        $items = $this->testModel->where('count', '>', 10)->get();

        $this->assertEquals(2, $items->count());

        // OR condition
        $items = $this->testModel
            ->where('count', '>', 100)
            ->orWhere('count', '>', 10)
            ->get();

        $this->assertEquals(2, $items->count());
    }

    public function testGetAllRecordsWithLTCondition()
    {
        $this->seed(['count' => ['N' => 10]]);
        $this->seed(['count' => ['N' => 9]]);
        $this->seed(['count' => ['N' => 11]]);

        $items = $this->testModel->where('count', '<', 10)->get();

        $this->assertEquals(1, $items->count());

        // OR condition
        $items = $this->testModel
            ->where('count', '<', 1)
            ->orWhere('count', '<', 10)
            ->get();

        $this->assertEquals(1, $items->count());
    }

    public function testGetAllRecordsWithGECondition()
    {
        $this->seed(['count' => ['N' => 10]]);
        $this->seed(['count' => ['N' => 9]]);
        $this->seed(['count' => ['N' => 11]]);

        $items = $this->testModel->where('count', '>=', 10)->get();

        $this->assertEquals(2, $items->count());

        // OR condition
        $items = $this->testModel
            ->where('count', '>=', 10)
            ->orWhere('count', '>=', 100)
            ->get();

        $this->assertEquals(2, $items->count());
    }

    public function testGetAllRecordsWithLECondition()
    {
        $this->seed(['count' => ['N' => 10]]);
        $this->seed(['count' => ['N' => 9]]);
        $this->seed(['count' => ['N' => 11]]);

        $items = $this->testModel->where('count', '<=', 10)->get();

        $this->assertEquals(2, $items->count());

        // OR condition
        $items = $this->testModel
            ->where('count', '<=', 10)
            ->orWhere('count', '<=', 1)
            ->get();

        $this->assertEquals(2, $items->count());
    }

    public function testGetAllRecordsWithBeginsWithOperator()
    {
        $this->seed(['description' => ['S' => 'Foo_1']]);
        $this->seed(['description' => ['S' => 'Foo_2']]);
        $this->seed(['description' => ['S' => 'Bar_Foo']]);

        $items = $this->testModel->where('description', 'BEGINS_WITH', 'Foo')->get();

        $this->assertEquals(2, $items->count());

        // OR condition
        $items = $this->testModel
            ->where('description', 'BEGINS_WITH', 'Foo')
            ->orWhere('description', 'BEGINS_WITH', 'Bar')
            ->get();

        $this->assertEquals(3, $items->count());
    }

    public function testGetAllRecordsWithContainsOperator()
    {
        $this->seed(['description' => ['L' => [['S' => 'foo'], ['S' => 'bar']]]]);
        $this->seed(['description' => ['L' => [['S' => 'foo'], ['S' => 'bar2']]]]);

        $items = $this->testModel->where('description', 'CONTAINS', 'foo')->get();

        $this->assertEquals(2, $items->count());

        $items = $this->testModel->where('description', 'CONTAINS', 'bar2')->get();

        $this->assertEquals(1, $items->count());

        // OR condition
        $items = $this->testModel
            ->where('description', 'CONTAINS', 'bar2')
            ->orWhere('description', 'CONTAINS', 'foo')
            ->get();

        $this->assertEquals(2, $items->count());
    }

    public function testGetAllRecordsWithNotContainsOperator()
    {
        $this->seed(['description' => ['L' => [['S' => 'foo'], ['S' => 'bar']]]]);
        $this->seed(['description' => ['L' => [['S' => 'foo'], ['S' => 'bar2']]]]);

        $items = $this->testModel->where('description', 'NOT_CONTAINS', 'foo')->get();

        $this->assertEquals(0, $items->count());

        $items = $this->testModel->where('description', 'NOT_CONTAINS', 'foobar')->get();

        $this->assertEquals(2, $items->count());

        // OR condition
        $items = $this->testModel
            ->where('description', 'NOT_CONTAINS', 'oo')
            ->orWhere('description', 'NOT_CONTAINS', 'aa')
            ->get();

        $this->assertEquals(2, $items->count());
    }

    public function testGetAllRecordsWithBetweenOperator()
    {
        $this->seed(['description' => ['N' => 10]]);
        $this->seed(['description' => ['N' => 11]]);

        $items = $this->testModel->where('description', 'BETWEEN', [1, 11])->get();

        $this->assertEquals(2, $items->count());

        $items = $this->testModel->where('description', 'BETWEEN', [100, 110])->get();

        $this->assertEquals(0, $items->count());

        // OR condition
        $items = $this->testModel
            ->where('description', 'BETWEEN', [100, 110])
            ->orWhere('description', 'BETWEEN', [1, 11])
            ->get();

        $this->assertEquals(2, $items->count());
    }

    public function testGetFirstRecord()
    {
        $firstItem = $this->seed();

        $items = $this->testModel->first();

        $this->assertEquals(Arr::get($firstItem, 'id.S'), $items->id);
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

        $this->assertEquals($this->marshaler->unmarshalItem($firstItem), $foundItems->first()->toArray());

        $foundItems = $this->testModel
            ->where('name', $secondItem['name']['S'])
            ->where('description', $secondItem['description']['S'])
            ->all();

        $this->assertEquals(1, $foundItems->count());

        $this->assertEquals($this->marshaler->unmarshalItem($secondItem), $foundItems->first()->toArray());
    }

    public function testLookUpByKey()
    {
        $this->seed();

        $item = $this->seed();

        $foundItems = $this->testModel->where('id', $item['id']['S'])->get();

        $this->assertEquals(1, $foundItems->count());

        $this->assertEquals($this->marshaler->unmarshalItem($item), $foundItems->first()->toArray());
    }

    public function testCount()
    {
        $this->seed();
        $this->seed(['name' => ['S' => 'Foo']]);
        $this->seed(['name' => ['S' => 'Foo']]);

        $this->assertEquals(3, $this->testModel->count());
        $this->assertEquals(1, $this->testModel->take(1)->count());
        $this->assertEquals(2, $this->testModel->where('name', 'Foo')->count());
    }

    public function testCountQuery()
    {
        $raw = $this->testModel->toDynamoDbQuery(['count(*)']);

        $this->assertEquals([
            'TableName' => $this->testModel->getTable(),
            'Select' => 'COUNT',
        ], $raw->query);
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
        $this->assertEquals(1, $barQuery->count());
        $this->assertEquals($this->marshaler->unmarshalItem($expectedFoo), $fooQuery->first()->toArray());
        $this->assertEquals($this->marshaler->unmarshalItem($expectedBar), $barQuery->first()->toArray());
    }

    public function testWhereIn()
    {
        $this->seed(['name' => ['S' => 'foo']]);
        $this->seed(['name' => ['S' => 'foo']]);
        $this->seed(['name' => ['S' => 'bar']]);
        $this->seed(['name' => ['S' => 'foobar']]);

        $items = $this->testModel->whereIn('name', ['foo', 'bar'])->get();

        $this->assertEquals(3, $items->count());

        foreach ($items as $item) {
            $this->assertContains($item->name, ['foo', 'bar']);
        }

        // OR condition
        $items = $this->testModel
            ->whereIn('name', ['foo', 'bar'])
            ->orWhereIn('name', ['foobar'])
            ->get();

        $this->assertEquals(4, $items->count());

        foreach ($items as $item) {
            $this->assertContains($item->name, ['foo', 'bar', 'foobar']);
        }
    }

    public function testWhereNull()
    {
        $this->seed();
        $this->seed([], ['name']);

        $items = $this->testModel->whereNull('name')->get();

        $this->assertEquals(1, $items->count());

        $this->assertNull($items->first()->name);

        // OR condition
        $items = $this->testModel->whereNull('name')->orWhereNull('description')->get();

        $this->assertEquals(1, $items->count());

        $this->assertNull($items->first()->name);
    }

    public function testWhereNotNull()
    {
        $this->seed();
        $this->seed([], ['name']);

        $items = $this->testModel->whereNotNull('name')->get();

        $this->assertEquals(1, $items->count());

        $this->assertNotNull($items->first()->name);

        // OR condition
        $items = $this->testModel->whereNotNull('name')->orWhereNotNull('description')->get();

        $this->assertEquals(2, $items->count());
    }

    public function testChunkScan()
    {
        $this->seed(['name' => ['S' => 'Foo']]);
        $this->seed(['name' => ['S' => 'Foo2']]);
        $this->seed(['name' => ['S' => 'Foo3']]);

        $iteration = 1;

        $this->testModel->chunk(2, function ($results) use (&$iteration) {
            if ($iteration === 1) {
                $this->assertEquals(2, count($results));
            } elseif ($iteration === 2) {
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

    public function testChunkEarlyReturn()
    {
        $this->seed(['name' => ['S' => 'Foo']]);
        $this->seed(['name' => ['S' => 'Foo2']]);
        $this->seed(['name' => ['S' => 'Foo3']]);

        $iteration = 1;

        $this->testModel->chunk(2, function () use (&$iteration) {
            $iteration++;
            return false;
        });

        $this->assertEquals(2, $iteration);
    }

    public function testStaticMethods()
    {
        $item = $this->seed(['name' => ['S' => 'Foo'], 'description' => ['S' => 'Bar']]);

        $item = $this->marshaler->unmarshalItem($item);

        $klass = get_class($this->testModel);

        $this->assertEquals([$item], $klass::all()->toArray());

        $this->assertEquals(1, $klass::where('name', 'Foo')->where('description', 'Bar')->get()->count());

        $this->assertEquals($item, $klass::first()->toArray());

        $this->assertEquals($item, $klass::find($item['id'])->toArray());
    }

    public function testNestedConditions()
    {
        $this->seed(['name' => ['S' => 'Foo'], 'count' => ['N' => 0]]);
        $this->seed(['name' => ['S' => 'Foo'], 'count' => ['N' => 10]]);
        $this->seed(['name' => ['S' => 'FooFoo'], 'count' => ['N' => 20]]);

        $items = $this->testModel
            ->where('name', 'contains', 'Foo')
            ->where(function ($query) {
                $query->where('count', 0)->orWhere('count', 10);
            })
            ->get();

        $this->assertEquals(2, $items->count());

        foreach ($items as $item) {
            $this->assertEquals('Foo', $item->name);
        }
    }

    public function testConditionContainingIndexKeyAndNonIndexKey()
    {
        $this->seed([
            'name' => ['S' => 'Bar'],
            'count' => ['N' => 11],
        ]);

        $item = $this->seed([
            'name' => ['S' => 'Foo'],
            'count' => ['N' => 10],
        ]);

        $expectedItem = $this->marshaler->unmarshalItem($item);

        $foundItems = $this->testModel
            ->where('count', 10)
            ->where('name', 'Foo')
            ->get();

        $this->assertEquals(1, $foundItems->count());
        $this->assertEquals($expectedItem, $foundItems->first()->toArray());
    }

    public function testSerialize()
    {
        $item = $this->marshaler->unmarshalItem($this->seed());
        $serializedItems = serialize($this->testModel->all());
        $unserializedItems = unserialize($serializedItems);

        $this->assertEquals([$item], $unserializedItems->toArray());
        $this->assertInstanceOf(get_class($this->testModel), $unserializedItems->first());
    }

    public function testLimit()
    {
        $count = 10;
        for ($i = 0; $i < $count; $i++) {
            $this->seed(['name' => ['S' => 'foo']]);
        }

        $items = $this->testModel->take(3)->get();
        $this->assertEquals(3, $items->count());

        $items = $this->testModel->limit(3)->get();
        $this->assertEquals(3, $items->count());

        // Ensure "limit" is reset in new queries
        $items = $this->testModel->get();
        $this->assertEquals($count, $items->count());

        $items = $this->testModel->where('name', 'foo')->take(4)->get();
        $this->assertEquals(4, $items->count());
    }

    public function testRemoveNestedAttribute()
    {
        $this->seed(['id' => ['S' => 'foo']]);

        $this->testModel
            ->where('id', 'foo')
            ->removeAttribute('nested.foo');

        $this->assertArrayNotHasKey('foo', $this->testModel->nested);
        $this->assertFalse($this->testModel->isDirty());

        $item = $this->testModel->find('foo');
        $this->assertArrayNotHasKey('foo', $item->nested);
    }

    public function testRemoveAttributesOnQuery()
    {
        $this->seed(['id' => ['S' => 'foo']]);

        $this->testModel
            ->where('id', 'foo')
            ->removeAttribute('description', 'name', 'nested.foo', 'nested.nestedArray[0]', 'nestedArray[0]');

        $this->assertRemoveAttributes($this->testModel);
        $this->assertFalse($this->testModel->isDirty());

        $item = $this->testModel->find('foo');
        $this->assertRemoveAttributes($item);
    }

    public function testRemoveAttributesOnModel()
    {
        $this->seed(['id' => ['S' => 'foo']]);

        $item = $this->testModel->first();
        $item->removeAttribute('description', 'name', 'nested.foo', 'nested.nestedArray[0]', 'nestedArray[0]');

        $this->assertRemoveAttributes($item);
        $this->assertFalse($item->isDirty());

        $item = $this->testModel->first();
        $this->assertRemoveAttributes($item);
    }

    public function testAfterForQueryOperation()
    {
        for ($i = 0; $i < 10; $i++) {
            $this->seed(['count' => ['N' => 10]]);
        }

        // Paginate 2 items at a time
        $pageSize = 2;
        $last = null;
        $paginationResult = collect();

        do {
            $items = $this->testModel
                ->where('count', 10)
                ->after($last)
                ->limit($pageSize)->all();
            $last = $items->last();
            $paginationResult = $paginationResult->merge($items->pluck('count'));
        } while ($last);

        $this->assertCount(10, $paginationResult);
        $paginationResult->each(function ($count) {
            $this->assertEquals(10, $count);
        });
    }

    public function testAfterForScanOperation()
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
            $this->seed(['count' => ['N' => 10]]);
        }

        $paginationResult = collect();
        $afterKey = null;

        do {
            $items = $this->testModel
                ->where('count', 10)
                ->afterKey($afterKey)
                ->limit(2)->all();
            $paginationResult = $paginationResult->merge($items->pluck('count'));
            $afterKey = $items->lastKey();
        } while ($afterKey);

        $this->assertCount(10, $paginationResult);
        $paginationResult->each(function ($count) {
            $this->assertEquals(10, $count);
        });
    }

    public function testAfterKeyForScanOperation()
    {
        foreach (range(0, 9) as $i) {
            $this->seed(['count' => ['N' => $i]]);
        }

        $assert = function (callable $getKey) {
            $paginationResult = collect();
            $afterKey = null;

            do {
                $items = $this->testModel
                    ->afterKey($afterKey)
                    ->limit(2)->all();
                $afterKey = $getKey($items);
                $paginationResult = $paginationResult->merge($items->pluck('count'));
            } while ($afterKey);

            $this->assertEquals(range(0, 9), $paginationResult->sort()->values()->toArray());
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
            $this->seed(['count' => ['N' => $i]]);
        }

        $items = $this->testModel
            ->decorate(function (RawDynamoDbQuery $raw) {
                $raw->op = 'Scan';
                $raw->query['FilterExpression'] = '#count > :count';
                $raw->query['ExpressionAttributeNames'] = [
                    '#count' => 'count'
                ];
                $raw->query['ExpressionAttributeValues'] = [
                    ':count' => ['N' => 0],
                ];
            })
            ->get();

        $this->assertEquals(range(1, 9), $items->pluck('count')->sort()->values()->toArray());
    }

    public function testToDynamoDbQueryWithDecorate()
    {
        $queryWithoutDecorate = $this->testModel->where('foo', 'bar');

        $this->assertEquals(
            [
                'FilterExpression' => '#foo = :a1',
                'ExpressionAttributeNames' => ['#foo' => 'foo'],
                'ExpressionAttributeValues' => [':a1' => ['S' => 'bar']],
                'TableName' => $this->testModel->getTable(),
            ],
            $queryWithoutDecorate->toDynamoDbQuery()->query
        );

        $queryWithDecorate = $queryWithoutDecorate->clone()
            ->decorate(function (RawDynamoDbQuery $raw) {
                $raw->query['FilterExpression'] .= ' AND extra_col = :extra_col_val';
                $raw->query['ExpressionAttributeValues'][':extra_col_val'] = ['N' => 0];
            });

        $this->assertEquals(
            [
                'FilterExpression' => '#foo = :a1 AND extra_col = :extra_col_val',
                'ExpressionAttributeNames' => ['#foo' => 'foo'],
                'ExpressionAttributeValues' => [':a1' => ['S' => 'bar'], ':extra_col_val' => ['N' => 0]],
                'TableName' => $this->testModel->getTable(),
            ],
            $queryWithDecorate->toDynamoDbQuery()->query
        );
    }

    private function assertUsingKeyAndFilterConditions($model)
    {
        foreach (range(0, 9) as $i) {
            $this->seed([
                'id' => ['S' => "$i"],
                'count' => ['N' => $i],
            ]);
        }

        $query = $model
            ->where('id', '8')
            ->where('count', '<=', 8);

        $dynamoDbQuery = $query->toDynamoDbQuery();

        $this->assertEquals('Query', $dynamoDbQuery->op);

        $this->assertEquals(
            '#id = :a1',
            $dynamoDbQuery->query['KeyConditionExpression']
        );

        $this->assertEquals(
            '#count <= :a2',
            $dynamoDbQuery->query['FilterExpression']
        );

        $result = $query->get();
        $this->assertEquals([8], $result->pluck('count')->toArray());
        $this->assertEquals(['8'], $result->pluck('id')->toArray());
    }

    public function testUsingBothKeyAndFilterConditionsForModelWithoutIndex()
    {
        $this->assertUsingKeyAndFilterConditions(new PrimaryKeyWithoutIndexModel());
    }

    public function testUsingBothKeyAndFilterConditionsForModelWithIndex()
    {
        $this->assertUsingKeyAndFilterConditions(new PrimaryKeyWithIndexModel());
    }

    public function testRefresh()
    {
        $this->seed();

        $model = $this->testModel->first();

        $originalName = $model->name;

        $model->name = 'Modified Name';

        $refreshed = $model->refresh();

        $this->assertEquals($originalName, $model->name);
        $this->assertEquals($originalName, $model->name);
        $this->assertEquals($refreshed, $model);
    }

    public function testQueryWithColumns()
    {
        $this->seed([
            'id' => ['S' => 'foo'],
            'name' => ['S' => 'bar'],
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
            'name' => 'bar',
            'nested' => ['key1' => 'value1'],
        ];

        $results = $this->testModel
            ->take(1)
            ->get($columns);

        $this->assertEquals($expected, $results->first()->toArray());

        $results = $this->testModel
            ->where('id', 'foo')
            ->get($columns);

        $this->assertEquals($expected, $results->first()->toArray());

        $result = $this->testModel
            ->first($columns);

        $this->assertEquals($expected, $result->toArray());

        $result = $this->testModel
            ->firstOrFail($columns);

        $this->assertEquals($expected, $result->toArray());
    }

    public function testFindWithColumns()
    {
        $this->seed([
            'id' => ['S' => 'foo'],
            'name' => ['S' => 'bar'],
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
            'name' => 'bar',
            'nested' => ['key1' => 'value1'],
        ];

        $result = $this->testModel
            ->find('foo', $columns);

        $this->assertEquals($expected, $result->toArray());
    }

    public function testQueryNestedAttributes()
    {
        $item = $this->seed([
            'nested' => [
                'M' => [
                    'foo' => ['S' => 'bar'],
                ],
            ],
            'nestedArray' => [
                'L' => [
                    ['S' => 'first'],
                    [
                        'M' => [
                            'foo' => ['S' => 'bar'],
                        ],
                    ],
                ],
            ],
        ]);

        $results = $this->testModel->where('nested.foo', 'bar')->all();
        $this->assertCount(1, $results);
        $this->assertEquals($item['id']['S'], $results->first()->id);

        $results = $this->testModel->where('nestedArray[0]', 'first')->all();
        $this->assertCount(1, $results);
        $this->assertEquals($item['id']['S'], $results->first()->id);

        $results = $this->testModel->where('nestedArray[1].foo', 'bar')->all();
        $this->assertCount(1, $results);
        $this->assertEquals($item['id']['S'], $results->first()->id);
    }

    public function testBuilderContainsAllWhereClausesWhenGivenArrayOfConditions()
    {
        /** @var array $conditions */
        $conditions = [
            "foo" => "bar",
            "bin" => "baz"
        ];

        $builder = $this->getTestModel()->where($conditions);

        /** @var array $conditionsFromBuilder */
        $conditionsFromBuilder = [];

        /** @var array $builderConditions */
        foreach ($builder->wheres as $builderConditions) {
            $conditionsFromBuilder[$builderConditions['column']] = $builderConditions['value'];
        }

        // Assert that the builder has the where-conditions we expect to see
        $this->assertEquals($conditions, $conditionsFromBuilder);
    }

    protected function assertRemoveAttributes($item)
    {
        $this->assertNull($item->name);
        $this->assertNull($item->description);
        $this->assertArrayNotHasKey('foo', $item->nested);
        $this->assertCount(0, $item->nested['nestedArray']);
        $this->assertCount(1, $item->nestedArray);
        $this->assertNotContains('first', $item->nestedArray);
        $this->assertNotNull($item->nested['hello']);
        $this->assertNotNull($item->count);
        $this->assertNotNull($item->author);
    }

    public function seed($attributes = [], $exclude = [])
    {
        $item = [
            'id' => ['S' => Str::random(36)],
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
class PrimaryKeyWithIndexModel extends DynamoDbModel
{
    protected $fillable = [
        'name',
        'description',
        'count',
        'author',
        'nested',
        'nestedArray',
    ];

    protected $table = 'test_model';

    protected $connection = 'test';

    protected $dynamoDbIndexKeys = [
        'count_index' => [
            'hash' => 'count',
        ],
    ];
}

class PrimaryKeyWithoutIndexModel extends DynamoDbModel
{
    protected $fillable = [
        'name',
        'description',
        'count',
        'author',
        'nested',
        'nestedArray',
    ];

    protected $table = 'test_model';

    protected $connection = 'test';
}
// phpcs:enable PSR1.Classes.ClassDeclaration.MultipleClasses
