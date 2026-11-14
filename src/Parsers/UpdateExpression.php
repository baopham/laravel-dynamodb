<?php

namespace BaoPham\DynamoDb\Parsers;

use BaoPham\DynamoDb\Facades\DynamoDb;

class UpdateExpression
{
    /**
     * @var ExpressionAttributeNames
     */
    protected $names;

    /**
     * @var ExpressionAttributeValues
     */
    protected $values;

    /**
     * @var Placeholder
     */
    protected $placeholder;

    public function __construct(
        ExpressionAttributeNames $names,
        ?ExpressionAttributeValues $values = null,
        ?Placeholder $placeholder = null
    ) {
        $this->names = $names;
        $this->values = $values ?: new ExpressionAttributeValues();
        $this->placeholder = $placeholder ?: new Placeholder();
    }

    public function reset()
    {
        $this->names->reset();
        $this->values->reset();
        $this->placeholder->reset();
    }

    public function set(array $attributes)
    {
        if (empty($attributes)) {
            return '';
        }

        $assignments = [];

        foreach ($attributes as $attribute => $value) {
            $this->names->set($attribute);
            $placeholder = $this->placeholder->next();
            $this->values->set($placeholder, DynamoDb::marshalValue($value));
            $assignments[] = sprintf('%s = :%s', $this->names->placeholder($attribute), $placeholder);
        }

        return 'SET ' . implode(', ', $assignments);
    }

    public function remove(array $attributes)
    {
        foreach ($attributes as $attribute) {
            $this->names->set($attribute);
        }

        return 'REMOVE ' . implode(', ', $this->names->placeholders());
    }
}
