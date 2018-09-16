<?php

namespace BaoPham\DynamoDb;

use Exception;
use DateTime;
use Illuminate\Database\Eloquent\Model;

/**
 * Class DynamoDbModel.
 */
abstract class DynamoDbModel extends Model
{
    /**
     * Always set this to false since DynamoDb does not support incremental Id.
     *
     * @var bool
     */
    public $incrementing = false;

    /**
     * @var \BaoPham\DynamoDb\DynamoDbClientInterface
     */
    protected static $dynamoDb;

    /**
     * @deprecated
     * @var \Aws\DynamoDb\Marshaler
     */
    protected $marshaler;

    /**
     * @deprecated
     * @var \BaoPham\DynamoDb\EmptyAttributeFilter
     */
    protected $attributeFilter;

    /**
     * Indexes.
     *   [
     *     '<simple_index_name>' => [
     *          'hash' => '<index_key>'
     *     ],
     *     '<composite_index_name>' => [
     *          'hash' => '<index_hash_key>',
     *          'range' => '<index_range_key>'
     *     ],
     *   ]
     *
     * @var array
     */
    protected $dynamoDbIndexKeys = [];

    /**
     * Array of your composite key.
     * ['<hash>', '<range>']
     *
     * @var array
     */
    protected $compositeKey = [];

    /**
     * Default Date format
     * ISO 8601 Compliant
     */
    protected $dateFormat = DateTime::ATOM;


    public function __construct(array $attributes = [])
    {
        $this->bootIfNotBooted();

        $this->syncOriginal();

        $this->fill($attributes);

        $this->setupDynamoDb();
    }

    /**
     * Get the DynamoDbClient service that is being used by the models.
     *
     * @return DynamoDbClientInterface
     */
    public static function getDynamoDbClientService()
    {
        return static::$dynamoDb;
    }

    /**
     * Set the DynamoDbClient used by models.
     *
     * @param DynamoDbClientInterface $dynamoDb
     *
     * @return void
     */
    public static function setDynamoDbClientService(DynamoDbClientInterface $dynamoDb)
    {
        static::$dynamoDb = $dynamoDb;
    }

    /**
     * Unset the DynamoDbClient service for models.
     *
     * @return void
     */
    public static function unsetDynamoDbClientService()
    {
        static::$dynamoDb = null;
    }

    protected function setupDynamoDb()
    {
        $this->marshaler = static::$dynamoDb->getMarshaler();
        $this->attributeFilter = static::$dynamoDb->getAttributeFilter();
    }

    public function newCollection(array $models = [], $index = null)
    {
        return new DynamoDbCollection($models, $index);
    }

    public function save(array $options = [])
    {
        $create = !$this->exists;

        if ($this->fireModelEvent('saving') === false) {
            return false;
        }

        if ($create && $this->fireModelEvent('creating')  === false) {
            return false;
        }

        if (!$create && $this->fireModelEvent('updating') === false) {
            return false;
        }

        if ($this->usesTimestamps()) {
            $this->updateTimestamps();
        }

        $saved = $this->newQuery()->save();

        if (!$saved) {
            return $saved;
        }

        $this->exists = true;
        $this->wasRecentlyCreated = $create;
        $this->fireModelEvent($create ? 'created' : 'updated', false);

        $this->finishSave($options);

        return $saved;
    }

    /**
     * Saves the model to DynamoDb asynchronously and returns a promise
     * @param array $options
     * @return bool|\GuzzleHttp\Promise\Promise
     */
    public function saveAsync(array $options = [])
    {
        $create = !$this->exists;

        if ($this->fireModelEvent('saving') === false) {
            return false;
        }

        if ($create && $this->fireModelEvent('creating')  === false) {
            return false;
        }

        if (!$create && $this->fireModelEvent('updating') === false) {
            return false;
        }

        if ($this->usesTimestamps()) {
            $this->updateTimestamps();
        }

        $savePromise = $this->newQuery()->saveAsync();

        $savePromise->then(function ($result) use ($create, $options) {
            if (array_get($result, '@metadata.statusCode') === 200) {
                $this->exists = true;
                $this->wasRecentlyCreated = $create;
                $this->fireModelEvent($create ? 'created' : 'updated', false);

                $this->finishSave($options);
            }
        });

        return $savePromise;
    }

    public function update(array $attributes = [], array $options = [])
    {
        return $this->fill($attributes)->save();
    }

    public function updateAsync(array $attributes = [], array $options = [])
    {
        return $this->fill($attributes)->saveAsync($options);
    }

    public static function create(array $attributes = [])
    {
        $model = new static;

        $model->fill($attributes)->save();

        return $model;
    }

    public function delete()
    {
        if (is_null($this->getKeyName())) {
            throw new Exception('No primary key defined on model.');
        }

        if ($this->exists) {
            if ($this->fireModelEvent('deleting') === false) {
                return false;
            }

            $this->exists = false;

            $success = $this->newQuery()->delete();

            if ($success) {
                $this->fireModelEvent('deleted', false);
            }

            return $success;
        }
    }

    public function deleteAsync()
    {
        if (is_null($this->getKeyName())) {
            throw new Exception('No primary key defined on model.');
        }

        if ($this->exists) {
            if ($this->fireModelEvent('deleting') === false) {
                return false;
            }

            $this->exists = false;

            $deletePromise = $this->newQuery()->deleteAsync();

            $deletePromise->then(function () {
                $this->fireModelEvent('deleted', false);
            });

            return $deletePromise;
        }
    }

    public static function all($columns = [])
    {
        $instance = new static;

        return $instance->newQuery()->get($columns);
    }

    public function refresh()
    {
        if (! $this->exists) {
            return $this;
        }

        $query = $this->newQuery();

        $refreshed = $query->find($this->getKeys());

        $this->setRawAttributes($refreshed->toArray());

        return $this;
    }

    /**
     * @return DynamoDbQueryBuilder
     */
    public function newQuery()
    {
        $builder = new DynamoDbQueryBuilder($this);

        foreach ($this->getGlobalScopes() as $identifier => $scope) {
            $builder->withGlobalScope($identifier, $scope);
        }

        return $builder;
    }

    public function hasCompositeKey()
    {
        return !empty($this->compositeKey);
    }

    /**
     * @deprecated
     * @param $item
     * @return array
     */
    public function marshalItem($item)
    {
        return $this->marshaler->marshalItem($item);
    }

    /**
     * @deprecated
     * @param $value
     * @return array
     */
    public function marshalValue($value)
    {
        return $this->marshaler->marshalValue($value);
    }

    /**
     * @deprecated
     * @param $item
     * @return array|\stdClass
     */
    public function unmarshalItem($item)
    {
        return $this->marshaler->unmarshalItem($item);
    }

    public function setId($id)
    {
        if (!is_array($id)) {
            $this->setAttribute($this->getKeyName(), $id);

            return $this;
        }

        foreach ($id as $keyName => $value) {
            $this->setAttribute($keyName, $value);
        }

        return $this;
    }

    /**
     * @return \Aws\DynamoDb\DynamoDbClient
     */
    public function getClient()
    {
        return static::$dynamoDb->getClient($this->connection);
    }

    /**
     * Get the value of the model's primary key.
     *
     * @return mixed
     */
    public function getKey()
    {
        return $this->getAttribute($this->getKeyName());
    }

    /**
     * Get the value of the model's primary / composite key.
     * Use this if you always want the key values in associative array form.
     *
     * @return array
     *
     * ['id' => 'foo']
     *
     * or
     *
     * ['id' => 'foo', 'id2' => 'bar']
     */
    public function getKeys()
    {
        if ($this->hasCompositeKey()) {
            $key = [];

            foreach ($this->compositeKey as $name) {
                $key[$name] = $this->getAttribute($name);
            }

            return $key;
        }

        $name = $this->getKeyName();

        return [$name => $this->getAttribute($name)];
    }

    /**
     * Get the primary key for the model.
     *
     * @return string
     */
    public function getKeyName()
    {
        return $this->primaryKey;
    }

    /**
     * Get the primary/composite key for the model.
     *
     * @return array
     */
    public function getKeyNames()
    {
        return $this->hasCompositeKey() ? $this->compositeKey : [$this->primaryKey];
    }

    /**
     * @return array
     */
    public function getDynamoDbIndexKeys()
    {
        return $this->dynamoDbIndexKeys;
    }

    /**
     * @param array $dynamoDbIndexKeys
     */
    public function setDynamoDbIndexKeys($dynamoDbIndexKeys)
    {
        $this->dynamoDbIndexKeys = $dynamoDbIndexKeys;
    }

    /**
     * @deprecated
     * @return \Aws\DynamoDb\Marshaler
     */
    public function getMarshaler()
    {
        return $this->marshaler;
    }

    /**
     * Remove non-serializable properties when serializing.
     *
     * @return array
     */
    public function __sleep()
    {
        return array_keys(
            array_except(get_object_vars($this), ['marshaler', 'attributeFilter'])
        );
    }

    /**
     * When a model is being unserialized, check if it needs to be booted and setup DynamoDB.
     *
     * @return void
     */
    public function __wakeup()
    {
        parent::__wakeup();
        $this->setupDynamoDb();
    }
}
