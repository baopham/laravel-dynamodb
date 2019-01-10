<?php

namespace BaoPham\DynamoDb\Tests;

use BaoPham\DynamoDb\DynamoDbModel;
use BaoPham\DynamoDb\DynamoDbQueryBuilder;

/**
 * Class MultipleWhereClauseTest
 * @package BaoPham\DynamoDb\Tests
 * @author Matthew Collins <matthew@instanceco.com>
 */
class MultipleWhereClauseTest extends DynamoDbTestCase
{
    public function testBuilderContainsAllWhereClausesWhenGivenArrayOfConditions()
    {
        /** @var DynamoDbQueryBuilder $builder */
        $builder = new DynamoDbQueryBuilder(new DynamoDbTestModel());

        $builder->where([
            "foo" => "bar",
            "bin" => "baz"
        ]);

        $this->assertEquals(2, count($builder->wheres));
    }
}

// phpcs:disable PSR1.Classes.ClassDeclaration.MultipleClasses
/**
 * Class DynamoDbTestModel
 * @package BaoPham\DynamoDb\Tests
 */
class DynamoDbTestModel extends DynamoDbModel
{
    /**
     * @var array
     */
    protected $fillable = [
        'foo',
        'bin'
    ];

    /**
     * @var string
     */
    protected $table = 'test';

    /**
     * @var string
     */
    protected $connection = 'test';
}
// phpcs:enable PSR1.Classes.ClassDeclaration.MultipleClasses
