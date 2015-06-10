<?php

namespace BaoPham\DynamoDb;

interface DynamoDbClientInterface
{
    function getClient();

    function getMarshaler();

    function getAttributeFilter();
}
