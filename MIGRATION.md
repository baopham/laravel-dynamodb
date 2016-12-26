Migration
================

From v0.3 to v0.4
------

There was a breaking change in the property `$dynamoDbIndexKeys` structure. In v0.3 and below, you would be using:

```php
/**
 * Indexes.
 * [
 *     'global_index_key' => 'global_index_name',
 *     'local_index_key' => 'local_index_name',
 * ]
 *
 * @var array
 */
protected $dynamoDbIndexKeys = [
    'count_index' => 'count',
];
```

To upgrade to v0.4, change it to:

```php
/**
 * Indexes.
 * [
 *     'simple_index_name' => [
 *          'hash' => 'index_key',
 *     ],
 *     'composite_index_name' => [
 *          'hash' => 'index_hash_key',
 *          'range' => 'index_range_key',
 *     ],
 * ]
 *
 * @var array
 */
protected $dynamoDbIndexKeys = [
    'count' => [
        'hash' => 'count_index',
    ],
];
```
