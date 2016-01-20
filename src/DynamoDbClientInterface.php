<?php

namespace BaoPham\DynamoDb;

interface DynamoDbClientInterface
{
    /**
     * @param string @name
     */
    function getClient($name = null);

    function getMarshaler();

    function getAttributeFilter();
}
