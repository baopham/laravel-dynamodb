<?php

namespace BaoPham\DynamoDb\Tests\Mocks;

use Aws\DynamoDb\DynamoDbClient;
use Aws\Result;

class DynamoDbClientMock extends DynamoDbClient
{
    public function putItem(array $args = []): Result {
        return parent::putItem($args);
    }

    public function updateItem(array $args = []): Result {
        return parent::updateItem($args);
    }

    public function deleteItem(array $args = []): Result {
        return parent::deleteItem($args);
    }

    public function scan(array $args = []): Result {
        return parent::scan($args);
    }

    public function query(array $args = []): Result {
        return parent::query($args);
    }

    public function batchWriteItem(array $args = []): Result {
        return parent::batchWriteItem($args);
    }
}
