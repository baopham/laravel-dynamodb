<?php

namespace BaoPham\DynamoDb\Concerns;

use BaoPham\DynamoDb\Parsers\ExpressionAttributeNames;
use BaoPham\DynamoDb\Parsers\ExpressionAttributeValues;
use BaoPham\DynamoDb\Parsers\FilterExpression;
use BaoPham\DynamoDb\Parsers\KeyConditionExpression;
use BaoPham\DynamoDb\Parsers\Placeholder;
use BaoPham\DynamoDb\Parsers\ProjectionExpression;
use BaoPham\DynamoDb\Parsers\UpdateExpression;

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
        $marshaler = $this->model->getMarshaler();

        $this->placeholder = new Placeholder();

        $this->expressionAttributeNames = new ExpressionAttributeNames();

        $this->expressionAttributeValues = new ExpressionAttributeValues();

        $this->keyConditionExpression = new KeyConditionExpression(
            $this->placeholder,
            $marshaler,
            $this->expressionAttributeValues,
            $this->expressionAttributeNames
        );

        $this->filterExpression = new FilterExpression(
            $this->placeholder,
            $marshaler,
            $this->expressionAttributeValues,
            $this->expressionAttributeNames
        );

        $this->projectionExpression = new ProjectionExpression();

        $this->updateExpression = new UpdateExpression($this->expressionAttributeNames);
    }

    public function resetExpressions()
    {
        $this->filterExpression->reset();
        $this->keyConditionExpression->reset();
        $this->updateExpression->reset();
    }
}
