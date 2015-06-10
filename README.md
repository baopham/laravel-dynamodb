laravel-dynamodb
================
(Only supports hash primary key type)

Install
------
Install service provider:

~~~php
// config/app.php

'providers' => [
    ...
    'BaoPham\DynamoDb\DynamoDbServiceProvider',
    ...
];
~~~

Usage
-----
* Extends your model with `BaoPham\DynamoDb\DynamoDbModel`, then you can use Eloquent methods that are supported.
* Or if you want to sync your DB table with a DynamoDb table, use trait `BaoPham\DynamoDb\ModelTrait`
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
