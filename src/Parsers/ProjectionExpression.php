<?php

namespace BaoPham\DynamoDb\Parsers;

class ProjectionExpression
{
    /**
     * @param array|string $columns
     * @return string
     */
    public function parse($columns)
    {
        return join(', ', $columns);
    }
}
