laravel-dynamodb
================
Only supports hash primary key type

Install
------
Install service provider:

~~~php
// config/app.php

'providers' => [
    ...
    BaoPham\DynamoDb\DynamoDbServiceProvider::class,
    ...
];
~~~

Usage
-----
* Extends your model with `BaoPham\DynamoDb\DynamoDbModel`, then you can use Eloquent methods that are supported. The idea here is that you can switch back to Eloquent without changing your queries.  

Supported methods:

~~~php
// find and delete
$model->find(<id>);
$model->delete();

// Using getIterator(). If 'key' is the same as primary key, will use 'Query', otherwise 'Scan'.
$model->where('key', 'key value')->get();

// See BaoPham\DynamoDb\ComparisonOperator - only tested with '=' and '!=' so far.
$model->where('key', '!=', 'key value');
$model->where(['key' => 'key value']);

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
~~~

* Or if you want to sync your DB table with a DynamoDb table, use trait `BaoPham\DynamoDb\ModelTrait`, it will call a `PutItem` after the model is saved.

* Put DynamoDb config in `config/services.php`:

~~~php
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
~~~

TODO
----
* Implement more methods for `DynamoDbModel`


Requirements:
-------------
Laravel 5.1

License:
--------
MIT

Author:
-------
Bao Pham
