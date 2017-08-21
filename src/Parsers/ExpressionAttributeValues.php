<?php

namespace BaoPham\DynamoDb\Parsers;

class ExpressionAttributeValues
{
    /**
     * @var array
     */
    protected $mapping;

    /**
     * @var string
     */
    protected $prefix;

    public function __construct($prefix = ':')
    {
        $this->reset();
        $this->prefix = $prefix;
    }

    public function set($placeholder, $value)
    {
        $this->mapping["{$this->prefix}{$placeholder}"] = $value;
    }

    public function get($placeholder)
    {
        return $this->mapping[$placeholder];
    }

    public function all()
    {
        return $this->mapping;
    }

    public function placeholders()
    {
        return array_keys($this->mapping);
    }

    public function reset()
    {
        $this->mapping = [];

        return $this;
    }
}
