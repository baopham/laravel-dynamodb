<?php

namespace Rennokki\DynamoDb\Concerns;

use Rennokki\DynamoDb\Parsers\ExpressionAttributeNames;
use Rennokki\DynamoDb\Parsers\ExpressionAttributeValues;
use Rennokki\DynamoDb\Parsers\FilterExpression;
use Rennokki\DynamoDb\Parsers\KeyConditionExpression;
use Rennokki\DynamoDb\Parsers\Placeholder;
use Rennokki\DynamoDb\Parsers\ProjectionExpression;
use Rennokki\DynamoDb\Parsers\UpdateExpression;

trait HasParsers
{
    /**
     * @var FilterExpression
     */
    protected $filterExpression;

    /**
     * @var KeyConditionExpression
     */
    protected $keyConditionExpression;

    /**
     * @var ProjectionExpression
     */
    protected $projectionExpression;

    /**
     * @var UpdateExpression
     */
    protected $updateExpression;

    /**
     * @var ExpressionAttributeNames
     */
    protected $expressionAttributeNames;

    /**
     * @var ExpressionAttributeValues
     */
    protected $expressionAttributeValues;

    /**
     * @var Placeholder
     */
    protected $placeholder;

    public function setupExpressions()
    {
        $this->placeholder = new Placeholder();

        $this->expressionAttributeNames = new ExpressionAttributeNames();

        $this->expressionAttributeValues = new ExpressionAttributeValues();

        $this->keyConditionExpression = new KeyConditionExpression(
            $this->placeholder,
            $this->expressionAttributeValues,
            $this->expressionAttributeNames
        );

        $this->filterExpression = new FilterExpression(
            $this->placeholder,
            $this->expressionAttributeValues,
            $this->expressionAttributeNames
        );

        $this->projectionExpression = new ProjectionExpression($this->expressionAttributeNames);

        $this->updateExpression = new UpdateExpression($this->expressionAttributeNames);
    }

    public function resetExpressions()
    {
        $this->filterExpression->reset();
        $this->keyConditionExpression->reset();
        $this->updateExpression->reset();
    }
}
