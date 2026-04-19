<?php namespace Baopham\Dynamodb\tests;

use Baopham\DynamoDb\DynamoDbCursorPaginator;
use Illuminate\Support\Str;

class DynamoDbCursorPaginatorTest extends DynamoDbModelTest
{

    protected function getTestModel()
    {
        return new TestModel([]);
    }

    public function testCreatePaginator()
    {
        $paginator = TestModel::paginate();
        $this->assertInstanceOf(DynamoDbCursorPaginator::class, $paginator);
    }

    public function testPageSizeLimit()
    {
        $this->seedMultiple(3);
        $paginator = TestModel::paginate(1);

        $this->assertCount(1, $paginator->items());
        $this->assertTrue($paginator->hasMorePages());
    }

    public function testNextPage()
    {
        $this->seed(['id' => ['S' => 'ONE']]);
        $this->seed(['id' => ['S' => 'TWO']]);
        $this->seed(['id' => ['S' => 'THREE']]);

        $paginator = TestModel::paginate(1);
        $this->assertCount(1, $paginator->items());
        $this->assertEquals('ONE', $paginator->items()[0]->id);
        $this->assertTrue($paginator->hasMorePages());

        $nextPaginator = TestModel::paginate(cursor: $paginator->nextCursor());

        $items = $nextPaginator->items();
        $this->assertCount(2, $items);
        $this->assertEquals('TWO', $items[0]->id);
        $this->assertFalse($nextPaginator->hasMorePages());
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
    public function seedMultiple($amount = 1)
    {
        for ($i = 0; $i < $amount; $i++) {
            $this->seed();
        }
    }
}

// phpcs:disable PSR1.Classes.ClassDeclaration.MultipleClasses
class TestModel extends \BaoPham\DynamoDb\DynamoDbModel
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
}
// phpcs:enable PSR1.Classes.ClassDeclaration.MultipleClasses
