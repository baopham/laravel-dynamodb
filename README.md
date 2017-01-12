laravel-dynamodb
================

[![Latest Stable Version](https://poser.pugx.org/baopham/dynamodb/v/stable)](https://packagist.org/packages/baopham/dynamodb)
[![Total Downloads](https://poser.pugx.org/baopham/dynamodb/downloads)](https://packagist.org/packages/baopham/dynamodb)
[![Latest Unstable Version](https://poser.pugx.org/baopham/dynamodb/v/unstable)](https://packagist.org/packages/baopham/dynamodb)
[![License](https://poser.pugx.org/baopham/dynamodb/license)](https://packagist.org/packages/baopham/dynamodb)

Supports all key types - primary hash key and composite keys.

> For advanced users only. If you're not familiar with Laravel, Laravel Eloquent and DynamoDB, then I suggest that you get familiar with those first. 

> **Breaking Changes** for v0.4
>  * If you're using v0.3 and below, please see [here](./README.v0.3.md)
>  * To upgrade to v0.4, please see the [migration note](./MIGRATION.md)

* [Install](#install)
* [Usage](#usage)
* [Indexes](#indexes)
* [Composite Keys](#composite-keys)
* [Test](#test)
* [Requirements](#requirements)
* [Todo](#todo)
* [License](#license)
* [Author and Contributors](#author-and-contributors)

Install
------

* Composer install
    ```bash
    composer require baopham/dynamodb
    ```

* Install service provider:

    ```php
    // config/app.php
    
    'providers' => [
        ...
        BaoPham\DynamoDb\DynamoDbServiceProvider::class,
        ...
    ];
    ```

* Put DynamoDb config in `config/services.php`:

    ```php
    // config/services.php
    ...
    'dynamodb' => [
        'key' => env('DYNAMODB_KEY'),
        'secret' => env('DYNAMODB_SECRET'),
        'region' => env('DYNAMODB_REGION'),
        'local_endpoint' => env('DYNAMODB_LOCAL_ENDPOINT'), // see http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Tools.DynamoDBLocal.html
        'local' => env('DYNAMODB_LOCAL'), // true or false? should use dynamodb_local or not?
    ],
    ...
    ```

Usage
-----
* Extends your model with `BaoPham\DynamoDb\DynamoDbModel`, then you can use Eloquent methods that are supported. The idea here is that you can switch back to Eloquent without changing your queries.  

Supported methods:

```php
// find and delete
$model->find(<id>);
$model->delete();

// Using getIterator(). If 'key' is the primary key or a global/local index and the condition is EQ, will use 'Query', otherwise 'Scan'.
$model->where('key', 'key value')->get();

// See BaoPham\DynamoDb\ComparisonOperator
$model->where(['key' => 'key value']);
// Chainable for 'AND'. 'OR' is not supported.
$model->where('foo', 'bar')
    ->where('foo2', '!=' 'bar2')
    ->get();

// Using scan operator, not too reliable since DynamoDb will only give 1MB total of data.
$model->all();

// Basically a scan but with limit of 1 item.
$model->first();

// update
$model->update($attributes);

$model = new Model();
// Define fillable attributes in your Model class.
$model->fillableAttr1 = 'foo';
$model->fillableAttr2 = 'foo';
// DynamoDb doesn't support incremented Id, so you need to use UUID for the primary key.
$model->id = 'de305d54-75b4-431b-adb2-eb6b9e546014'
$model->save();

// chunk
$model->chunk(10, function ($records) {
    foreach ($records as $record) {

    }
});
```

* Or if you want to sync your DB table with a DynamoDb table, use trait `BaoPham\DynamoDb\ModelTrait`, it will call a `PutItem` after the model is saved.

Indexes
-----------
If your table has indexes, make sure to declare them in your model class like so

```php
/**
 * Indexes.
 * [
 *     'simple_index_name' => [
 *          'hash' => 'index_key'
 *     ],
 *     'composite_index_name' => [
 *          'hash' => 'index_hash_key',
 *          'range' => 'index_range_key'
 *     ],
 * ].
 *
 * @var array
 */
protected $dynamoDbIndexKeys = [
    'count_index' => [
        'hash' => 'count'
    ],
];
```

Note that order of index matters when a key exists in multiple indexes.  
For example, we have this

```php
$this->where('user_id', 123)->where('count', '>', 10)->get();
```

with

```php
protected $dynamoDbIndexKeys = [
    'count_index' => [
        'hash' => 'user_id',
        'range' => 'count'
    ],
    'user_index' => [
        'hash' => 'user_id',
    ],
];
```

will use `count_index`.

```php
protected $dynamoDbIndexKeys = [
    'user_index' => [
        'hash' => 'user_id',
    ],
    'count_index' => [
        'hash' => 'user_id',
        'range' => 'count'
    ]
];
```

will use `user_index`.


Composite Keys
--------------
To use composite keys with your model:

* Set `$compositeKey` to an array of the attributes names comprising the key, e.g.

```php
protected $primaryKey = ['customer_id'];
protected $compositeKey = ['customer_id', 'agent_id'];
```

* To find a record with a composite key

```php
$model->find(['id1' => 'value1', 'id2' => 'value2']);
```

Test
----
Run:

```bash
$ java -Djava.library.path=./DynamoDBLocal_lib -jar dynamodb_local/DynamoDBLocal.jar --port 3000
$ ./vendor/bin/phpunit
```

* DynamoDb local version: 2016-01-07_1.0

* DynamoDb local schema for tests created by the [DynamoDb local shell](http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Tools.DynamoDBLocal.Shell.html) is located [here](dynamodb_local_schema.js)


Requirements
-------------
Laravel ^5.1


TODO
----
- [ ] Upgrade a few legacy attributes: `AttributesToGet`, `ScanFilter`, ...

License
--------
MIT


Author and Contributors
-------
* [Bao Pham](https://github.com/baopham/laravel-dynamodb)
* [warrick-loyaltycorp](https://github.com/warrick-loyaltycorp)
* [Alexander Ward](https://github.com/cthos)
* [Quang Ngo](https://github.com/vanquang9387)
