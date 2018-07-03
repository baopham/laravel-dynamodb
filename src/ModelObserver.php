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

    public function saved($model)
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
}
