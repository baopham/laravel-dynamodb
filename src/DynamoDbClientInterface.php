<?php

namespace BaoPham\DynamoDb;

interface DynamoDbClientInterface
{
    public function getClient($connection = null);

    public function getMarshaler();

    public function getAttributeFilter();
}
