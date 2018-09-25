<?php

namespace BaoPham\DynamoDb;

use Exception;
use Illuminate\Support\Facades\Log;

/**
 * Class ModelObserver.
 */
class ModelObserver
{
    /**
     * @var \Aws\DynamoDb\DynamoDbClient
     */
    protected $dynamoDbClient;

    /**
     * @var \Aws\DynamoDb\Marshaler
     */
    protected $marshaler;

    /**
     * @var \BaoPham\DynamoDb\EmptyAttributeFilter
     */
    protected $attributeFilter;

    public function __construct(DynamoDbClientInterface $dynamoDb)
    {
        $this->dynamoDbClient = $dynamoDb->getClient();
        $this->marshaler = $dynamoDb->getMarshaler();
        $this->attributeFilter = $dynamoDb->getAttributeFilter();
    }

    protected function _save($model)
    {
        $attrs = $model->attributesToArray();
        // $this->attributeFilter->filter($attrs);
        try {
            $this->dynamoDbClient->putItem([
                'TableName' => $model->getDynamoDbTableName(),
                'Item' => $this->marshaler->marshalItem($attrs),
            ]);
        } catch (Exception $e) {
            Log::info($e);
        }
    }

    protected function _delete($model)
    {
        $key = [$model->getKeyName() => $model->getKey()];

        try {
            $this->dynamoDbClient->deleteItem([
                'TableName' => $model->getDynamoDbTableName(),
                'Key' => $this->marshaler->marshalItem($key),
            ]);
        } catch (Exception $e) {
            Log::info($e);
        }
    }

    public function created($model)
    {
        $this->_save($model);
    }

    public function updated($model)
    {
        $this->_save($model);
    }

    public function deleted($model)
    {
        $this->_delete($model);
    }

    public function restored($model)
    {
        $this->_save($model);
    }
}
