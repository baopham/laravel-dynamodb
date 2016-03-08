<?php

namespace BaoPham\DynamoDb;

use Aws\DynamoDb\DynamoDbClient;
use Aws\DynamoDb\Marshaler;

class DynamoDbClientService implements DynamoDbClientInterface
{

    /**
     * @var \Aws\DynamoDb\DynamoDbClient
     */
    protected $client = [];

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
        if (array_key_exists('region', $config)) {
            $config = array(
                '__default__' => $config,
            );
        }
        foreach ($config as $name => $named_config) {
            $this->client[$name] = new DynamoDbClient($named_config);
            $this->marshaler = $marshaler;
            $this->attributeFilter = $filter;
        }
    }

    /**
     * @param string $name
     * @return \Aws\DynamoDb\DynamoDbClient
     */
    public function getClient($name = '__default__')
    {
        if (!array_key_exists($name, $this->client)) {
            $name = array_keys($this->client)[0];
        }
        return $this->client[$name];
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
