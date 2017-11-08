laravel-dynamodb
================

[![Latest Stable Version](https://poser.pugx.org/baopham/dynamodb/v/stable)](https://packagist.org/packages/baopham/dynamodb)
[![Total Downloads](https://poser.pugx.org/baopham/dynamodb/downloads)](https://packagist.org/packages/baopham/dynamodb)
[![Latest Unstable Version](https://poser.pugx.org/baopham/dynamodb/v/unstable)](https://packagist.org/packages/baopham/dynamodb)
[![Build Status](https://travis-ci.org/baopham/laravel-dynamodb.svg?branch=master)](https://travis-ci.org/baopham/laravel-dynamodb)
[![License](https://poser.pugx.org/baopham/dynamodb/license)](https://packagist.org/packages/baopham/dynamodb)

Supports all key types - primary hash key and composite keys.

> For advanced users only. If you're not familiar with Laravel, Laravel Eloquent and DynamoDB, then I suggest that you get familiar with those first. 

**Breaking changes in v2: config no longer lives in config/services.php**

* [Install](#install)
* [Usage](#usage)
  * [find() and delete()](#find-and-delete) 
  * [Conditions](#conditions)
  * [all() and first()](#all-and-first)
  * [Update](#update)
  * [Save](#save)
  * [Chunk](#chunk)
  * [limit() and take()](#limit-and-take)
  * [firstOrFail()](#firstorfail)
  * [findOrFail()](#findorfail)
  * [Query scope](#query-scope)
  * [REMOVE — Deleting Attributes From An Item](#remove--deleting-attributes-from-an-item)
* [Indexes](#indexes)
* [Composite Keys](#composite-keys)
* [Requirements](#requirements)
* [Migrate from v1 to v2](#migrate-from-v1-to-v2)
* [FAQ](#faq)
* [License](LICENSE)
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

* Run:

    ```php
    php artisan vendor:publish
    ```

* Update DynamoDb config in [config/dynamodb.php](config/dynamodb.php)


Usage
-----
* Extends your model with `BaoPham\DynamoDb\DynamoDbModel`, then you can use Eloquent methods that are supported. The idea here is that you can switch back to Eloquent without changing your queries.  
* Or if you want to sync your DB table with a DynamoDb table, use trait `BaoPham\DynamoDb\ModelTrait`, it will call a `PutItem` after the model is saved.

### Supported features:

#### find() and delete()

```php
$model->find(<id>);
$model->delete();
```

#### Conditions

```php
// Using getIterator()
// If 'key' is the primary key or a global/local index and it is a supported Query condition,
// will use 'Query', otherwise 'Scan'.
$model->where('key', 'key value')->get();

$model->where(['key' => 'key value']);

// Chainable for 'AND'.
$model->where('foo', 'bar')
    ->where('foo2', '!=' 'bar2')
    ->get();
    
// Chainable for 'OR'.
$model->where('foo', 'bar')
    ->orWhere('foo2', '!=' 'bar2')
    ->get();
 
// Other types of conditions
$model->where('count', '>', 0)->get();
$model->where('count', '>=', 0)->get();
$model->where('count', '<', 0)->get();
$model->where('count', '<=', 0)->get();
$model->whereIn('count', [0, 100])->get();
$model->whereNotIn('count', [0, 100])->get();
$model->where('count', 'between', [0, 100])->get();
$model->where('description', 'begins_with', 'foo')->get();
$model->where('description', 'contains', 'foo')->get();
$model->where('description', 'not_contains', 'foo')->get();

// Nested conditions
$model->where('name', 'foo')
    ->where(function ($query) {
        $query->where('count', 10)->orWhere('count', 20);
    });
```

##### whereNull() and whereNotNull()

> NULL and NOT_NULL only check for the attribute presence not its value being null  
> See: http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Condition.html  

```php
$model->whereNull('name');
$model->whereNotNull('name');
```

#### all() and first()

```php
// Using scan operator, not too reliable since DynamoDb will only give 1MB total of data.
$model->all();

// Basically a scan but with limit of 1 item.
$model->first();
```

#### update()

```php
// update
$model->update($attributes);
```

#### save()

```php
$model = new Model();
// Define fillable attributes in your Model class.
$model->fillableAttr1 = 'foo';
$model->fillableAttr2 = 'foo';
// DynamoDb doesn't support incremented Id, so you need to use UUID for the primary key.
$model->id = 'de305d54-75b4-431b-adb2-eb6b9e546014'
$model->save();
```

#### chunk()

```php
// chunk
$model->chunk(10, function ($records) {
    foreach ($records as $record) {

    }
});
```

#### limit() and take()

```php
// Use this with caution unless your limit is small.
// DynamoDB has a limit of 1MB so if your limit is very big, the results will not be expected.
$model->where('name', 'foo')->take(3)->get();
```

#### firstOrFail()

```php
$model->where('name', 'foo')->firstOrFail();
// for composite key
$model->where('id', 'foo')->where('id2', 'bar')->firstOrFail();
```

#### findOrFail()

```php
$model->findOrFail('foo');
// for composite key
$model->findOrFail(['id' => 'foo', 'id2' => 'bar']);
```

#### Query Scope

```php
class Foo extends DynamoDbModel
{
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

$foo = new Foo();
// Global scope will be applied
$foo->all();
// Local scope
$foo->withoutGlobalScopes()->countUnderFour()->get();
// Dynamic local scope
$foo->withoutGlobalScopes()->countUnder(6)->get();
```

#### REMOVE — Deleting Attributes From An Item

> See: http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.UpdateExpressions.html#Expressions.UpdateExpressions.REMOVE

```php
$model = new Model();
$model->where('id', 'foo')->removeAttribute('name', 'description', 'nested.foo', 'nestedArray[0]');

// Or
Model::find('foo')->removeAttribute('name', 'description', 'nested.foo', 'nestedArray[0]');
```

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

Requirements
-------------
Laravel ^5.1


Migrate from v1 to v2
---------------------

Follow these steps:

1. Update your `composer.json` to use v2
1. Run `composer update`
1. Run `php artisan vendor:publish`
1. Move your DynamoDb config in `config/services.php` to the new config file `config/dynamodb.php` as one of the connections
    1. Move `key`, `secret`, `token` inside `credentials`
    1. Rename `local_endpoint` to `endpoint`
    1. Remove `local` field


FAQ
---
Q: Cannot assign `id` property if its not in the fillable array  
A: Try [this](https://github.com/baopham/laravel-dynamodb/issues/10)?  


Q: How to create migration?  
A: Please see [this issue](https://github.com/baopham/laravel-dynamodb/issues/90)  



Author and Contributors
-------
* [Bao Pham](https://github.com/baopham/laravel-dynamodb)
* [warrick-loyaltycorp](https://github.com/warrick-loyaltycorp)
* [Alexander Ward](https://github.com/cthos)
* [Quang Ngo](https://github.com/vanquang9387)
* [David Higgins](https://github.com/zoul0813)
