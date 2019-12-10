<?php

namespace Rennokki\DynamoDb\Parsers;

class ProjectionExpression
{
    protected $names;

    public function __construct(ExpressionAttributeNames $names)
    {
        $this->names = $names;
    }

    /**
     * @param array $columns
     * @return string
     */
    public function parse(array $columns)
    {
        foreach ($columns as $column) {
            $this->names->set($column);
        }

        return implode(', ', $this->names->placeholders());
    }
}
