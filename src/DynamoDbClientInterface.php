<?php

namespace BaoPham\DynamoDb;

interface DynamoDbClientInterface
{
    public function getClient();

    public function getMarshaler();

    public function getAttributeFilter();
}
