<?php

namespace BaoPham\DynamoDb\Tests;

use Carbon\Carbon;
use BaoPham\DynamoDb\DynamoDbQueryBuilder;

/**
 * Class DynamoDbQueryScopeTest
 *
 * @package BaoPham\DynamoDb\Tests
 */
class DynamoDbQueryScopeTest extends ModelTest
{
    protected function getTestModel()
    {
        return new ModelWithQueryScopes([]);
    }

    public function testGlobalScope()
    {
        $seeds = [];
        for ($x = 0; $x < 10; $x++) {
            $seeds[] = $this->seed(['count' => ['N' => $x]]);
        }

        $items = $this->testModel->all();

        $this->assertCount(3, $items);

        foreach ($items as $item) {
            $this->assertGreaterThan(6, $item->count);
        }
    }

    public function testLocalScope()
    {
        $seeds = [];
        for ($x = 0; $x < 10; $x++) {
            $seeds[] = $this->seed(['count' => ['N' => $x]]);
        }

        $items = $this->testModel->withoutGlobalScopes()->countUnderFour()->get();

        $this->assertCount(4, $items);
    }

    public function testDynamicLocalScope()
    {
        $seeds = [];
        for ($x = 0; $x < 10; $x++) {
            $seeds[] = $this->seed(['count' => ['N' => $x]]);
        }

        $items = $this->testModel->withoutGlobalScopes()->countUnder(6)->get();

        $this->assertCount(6, $items);
    }

    public function testChunkWithGlobalScope()
    {
        for ($x = 0; $x < 350; $x++) {
            $this->seed(['count' => ['N' => $x]]);
        }

        // also test that it doesn't fail if there are more than 300 calls
        $this->testModel->chunk(1, function ($results) {
            $this->assertGreaterThan(6, $results->first()->count);
        });
    }

    public function testChunkWithLocalScope()
    {
        for ($x = 0; $x < 10; $x++) {
            $this->seed(['count' => ['N' => $x]]);
        }

        $this->testModel->withoutGlobalScopes()->countUnderFour()->chunk(1, function ($results) {
            $this->assertLessThan(4, $results->first()->count);
        });
    }

    public function testChunkWithDynamicLocalScope()
    {
        for ($x = 0; $x < 10; $x++) {
            $this->seed(['count' => ['N' => $x]]);
        }

        $this->testModel->withoutGlobalScopes()->countUnder(6)->chunk(1, function ($results) {
            $this->assertLessThan(6, $results->first()->count);
        });
    }

    public function seed($attributes = [])
    {
        $item = [
            'id' => ['S' => str_random(36)],
            'name' => ['S' => str_random(36)],
            'description' => ['S' => str_random(256)],
            'count' => ['N' => rand()],
            'author' => ['S' => str_random()],
        ];

        $item = array_merge($item, $attributes);

        $this->getClient()->putItem([
            'TableName' => $this->testModel->getTable(),
            'Item' => $item,
        ]);

        return $item;
    }
}

class ModelWithQueryScopes extends \BaoPham\DynamoDb\DynamoDbModel
{
    protected $fillable = ['name', 'description', 'count'];

    protected $table = 'test_model';

    public $timestamps = true;

    protected $dynamoDbIndexKeys = [
        'count_index' => [
            'hash' => 'count',
        ],
    ];

    protected static function boot()
    {
        parent::boot();

        static::addGlobalScope('count', function (DynamoDbQueryBuilder $builder) {
            $builder->where('count', '>', 6);
        });
    }

    public function scopeCountUnderFour($builder)
    {
        return $builder->where('count', '<', 4);
    }

    public function scopeCountUnder($builder, $count)
    {
        return $builder->where('count', '<', $count);
    }
}
