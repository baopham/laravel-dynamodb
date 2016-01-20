<?php

namespace BaoPham\DynamoDb;

interface DynamoDbClientInterface
{
    function getClient($name = null);

    function getMarshaler();

    function getAttributeFilter();
}
