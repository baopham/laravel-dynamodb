<?php

namespace BaoPham\DynamoDb\Tests;

use BaoPham\DynamoDb\DynamoDbQueryBuilder;
use Illuminate\Support\Str;

/**
 * Class DynamoDbQueryScopeTest
 *
 * @package BaoPham\DynamoDb\Tests
 */
class DynamoDbQueryScopeTest extends DynamoDbModelTestBase
{
    protected function getTestModel()
    {
        return new ModelWithQueryScopes([]);
    }

    public function testGlobalScope()
    {
        $this->seedMultiple();

        $items = $this->testModel->all();

        $this->assertNotCount(0, $items);

        foreach ($items as $item) {
            $this->assertGreaterThan(6, $item->count);
        }
    }

    public function testLocalScope()
    {
        $this->seedMultiple();

        $items = $this->testModel->withoutGlobalScopes()->countUnderFour()->get();

        $this->assertNotCount(0, $items);

        foreach ($items as $item) {
            $this->assertLessThan(4, $item->count);
        }
    }

    public function testDynamicLocalScope()
    {
        $this->seedMultiple();

        $items = $this->testModel->withoutGlobalScopes()->countUnder(6)->get();

        $this->assertNotCount(0, $items);

        foreach ($items as $item) {
            $this->assertLessThan(6, $item->count);
        }
    }

    public function testDifferentScopes()
    {
        $this->seedMultiple();

        // Dynamic local scope
        $items = $this->testModel->withoutGlobalScopes()->countUnder(6)->get();

        $this->assertNotCount(0, $items);

        foreach ($items as $item) {
            $this->assertLessThan(6, $item->count);
        }

        // Local scope
        $items = $this->testModel->withoutGlobalScopes()->countUnderFour()->get();

        $this->assertNotCount(0, $items);

        foreach ($items as $item) {
            $this->assertLessThan(4, $item->count);
        }

        // Global and local scope
        $items = $this->testModel->countUnderFour()->get();

        $this->assertCount(0, $items);
    }

    public function testChunkWithGlobalScope()
    {
        $this->seedMultiple(350);

        // also test that it doesn't fail if there are more than 300 calls
        $this->testModel->chunk(1, function ($results) {
            $this->assertGreaterThan(6, $results->first()->count);
        });
    }

    public function testChunkWithLocalScope()
    {
        $this->seedMultiple();

        $this->testModel->withoutGlobalScopes()->countUnderFour()->chunk(1, function ($results) {
            $this->assertLessThan(4, $results->first()->count);
        });
    }

    public function testChunkWithDynamicLocalScope()
    {
        $this->seedMultiple();

        $this->testModel->withoutGlobalScopes()->countUnder(6)->chunk(1, function ($results) {
            $this->assertLessThan(6, $results->first()->count);
        });
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

    protected function seedMultiple($count = 10)
    {
        for ($x = 0; $x < $count; $x++) {
            $this->seed(['count' => ['N' => $x]]);
        }
    }
}

// phpcs:disable PSR1.Classes.ClassDeclaration.MultipleClasses
class ModelWithQueryScopes extends \BaoPham\DynamoDb\DynamoDbModel
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
// phpcs:enable PSR1.Classes.ClassDeclaration.MultipleClasses
