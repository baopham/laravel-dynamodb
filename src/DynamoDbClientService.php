<?php

namespace BaoPham\DynamoDb;

use Aws\DynamoDb\DynamoDbClient;
use Aws\DynamoDb\Marshaler;

class DynamoDbClientService implements DynamoDbClientInterface
{
    /**
     * @var \Aws\DynamoDb\DynamoDbClient
     */
    protected $client;

    /**
     * @var \Aws\DynamoDb\Marshaler
     */
    protected $marshaler;

    /**
     * @var \BaoPham\DynamoDb\EmptyAttributeFilter
     */
    protected $attributeFilter;

    public function __construct($config, Marshaler $marshaler, EmptyAttributeFilter $filter)
    {
        $this->client = new DynamoDbClient($config);
        $this->marshaler = $marshaler;
        $this->attributeFilter = $filter;
    }

    /**
     * @return \Aws\DynamoDb\DynamoDbClient
     */
    public function getClient()
    {
        return $this->client;
    }

    /**
     * @return \Aws\DynamoDb\Marshaler
     */
    public function getMarshaler()
    {
        return $this->marshaler;
    }

    /**
     * @return \BaoPham\DynamoDb\EmptyAttributeFilter
     */
    public function getAttributeFilter()
    {
        return $this->attributeFilter;
    }
}
