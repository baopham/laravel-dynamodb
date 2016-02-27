laravel-dynamodb
================
Supports all key types - primary hash key and composite keys.

Install
------

```bash
composer require baopham/dynamodb:0.2.0
```

Install service provider:

```php
// config/app.php

'providers' => [
    ...
    BaoPham\DynamoDb\DynamoDbServiceProvider::class,
    ...
];
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
```

* Or if you want to sync your DB table with a DynamoDb table, use trait `BaoPham\DynamoDb\ModelTrait`, it will call a `PutItem` after the model is saved.

* Put DynamoDb config in `config/services.php`:

```php
// config/services.php
    ...
    'dynamodb' => [
        'key' => env('DYNAMODB_KEY'),
        'secret' => env('DYNAMODB_SECRET'),
        'region' => env('DYNAMODB_REGION'),
        'local_endpoint' => env('DYNAMODB_LOCAL_ENDPOINT') // see http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Tools.DynamoDBLocal.html
        'local' => env('DYNAMODB_LOCAL')
    ],
    ...
```

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

* DynamoDb local version: 2015-07-16_1.0

* DynamoDb local schema for tests created by the [DynamoDb local shell](http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Tools.DynamoDBLocal.Shell.html) is located [here](dynamodb_local_schema.js)


TODO
----
- [ ] Upgrade a few legacy attributes: `AttributesToGet`, `ScanFilter`, ...


Requirements:
-------------
Laravel ^5.1

License:
--------
MIT

Author and contributors
-------
* Bao Pham
* [warrick-loyaltycorp](https://github.com/warrick-loyaltycorp)
