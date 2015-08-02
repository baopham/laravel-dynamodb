laravel-dynamodb
================
Only supports hash primary key type

Install
------

```json
// composer.json
"require": {
    "baopham/dynamodb": "dev-master"
}
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

Test
----
Run:

```bash
$ java -Djava.library.path=./DynamoDBLocal_lib -jar dynamodb_local/DynamoDBLocal.jar --port 3000
$ ./vendor/bin/phpunit
```

This is the [test table created for DynamoDb local by the DynamoDb local shell](http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Tools.DynamoDBLocal.Shell.html)

```javascript
var params = {
    TableName: 'test_model',
    KeySchema: [ // The type of of schema.  Must start with a HASH type, with an optional second RANGE.
        { // Required HASH type attribute
            AttributeName: 'id',
            KeyType: 'HASH',
        }
    ],
    AttributeDefinitions: [ // The names and types of all primary and index key attributes only
        {
            AttributeName: 'id',
            AttributeType: 'S', // (S | N | B) for string, number, binary
        },
        {
            AttributeName: 'count',
            AttributeType: 'N', // (S | N | B) for string, number, binary
        }
    ],
    ProvisionedThroughput: { // required provisioned throughput for the table
        ReadCapacityUnits: 1, 
        WriteCapacityUnits: 1, 
    },
    GlobalSecondaryIndexes: [ // optional (list of GlobalSecondaryIndex)
        { 
            IndexName: 'count_index', 
            KeySchema: [
                { // Required HASH type attribute
                    AttributeName: 'count',
                    KeyType: 'HASH',
                }
            ],
            Projection: { // attributes to project into the index
                ProjectionType: 'ALL', // (ALL | KEYS_ONLY | INCLUDE)
            },
            ProvisionedThroughput: { // throughput to provision to the index
                ReadCapacityUnits: 1,
                WriteCapacityUnits: 1,
            },
        },
        // ... more global secondary indexes ...
    ]
};
dynamodb.createTable(params, function(err, data) {
    if (err) print(err); // an error occurred
    else print(data); // successful response
});
```

TODO
----
* Upgrade a few legacy attributes: `AttributesToGet`, `ScanFilter`, ...


Requirements:
-------------
Laravel 5.1

License:
--------
MIT

Author:
-------
Bao Pham
