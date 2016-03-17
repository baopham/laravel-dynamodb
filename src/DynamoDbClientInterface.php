<?php

namespace BaoPham\DynamoDb;

interface DynamoDbClientInterface
{
    /**
     * @param string @name
     */
    public function getClient($name = null);

    public function getMarshaler();

    public function getAttributeFilter();
}
