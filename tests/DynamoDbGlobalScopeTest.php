<?php

namespace BaoPham\DynamoDb\Tests;

use Carbon\Carbon;
use BaoPham\DynamoDb\DynamoDbQueryBuilder;

/**
 * Class DynamoDbTimestampTest
 *
 * @package BaoPham\DynamoDb\Tests
 */
class DynamoDbGlobalScopeTest extends ModelTest
{
    protected function getTestModel()
    {
        return new GlobalScopeModel([]);
    }

    public function testGlobalScope()
    {
        $seeds = [];
        for ($x = 0; $x < 10; $x++) {
            $seeds[] = $this->seed(['count'=>['N' => $x]]);
        }

        $items = $this->testModel->all();

        $this->assertEquals(3, count($items));
    }

    public function testLocalScope()
    {
        $seeds = [];
        for ($x = 0; $x < 10; $x++) {
            $seeds[] = $this->seed(['count'=>['N' => $x]]);
        }

        $items = $this->testModel->withoutGlobalScopes()->countUnderFour()->get();

        $this->assertEquals(4, count($items));
    }

    public function testDynamicLocalScope()
    {
        $seeds = [];
        for ($x = 0; $x < 10; $x++) {
            $seeds[] = $this->seed(['count'=>['N' => $x]]);
        }

        $items = $this->testModel->withoutGlobalScopes()->countUnder(6)->get();

        $this->assertEquals(6, count($items));
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

class GlobalScopeModel extends \BaoPham\DynamoDb\DynamoDbModel
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

        static::addGlobalScope('count', function (DynamoDbQueryBuilder $builder) 
        {
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
