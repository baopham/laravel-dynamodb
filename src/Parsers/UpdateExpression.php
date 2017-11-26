<?php

namespace BaoPham\DynamoDb\Parsers;

class UpdateExpression
{
    /**
     * @var ExpressionAttributeNames
     */
    protected $names;

    public function __construct(ExpressionAttributeNames $names)
    {
        $this->names = $names;
    }

    public function reset()
    {
        $this->names->reset();
    }

    public function remove(array $attributes)
    {
        foreach ($attributes as $attribute) {
            $this->names->set($attribute);
        }

        return 'REMOVE ' . implode(', ', $this->names->placeholders());
    }
}
