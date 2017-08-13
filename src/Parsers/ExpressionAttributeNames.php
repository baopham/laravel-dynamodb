<?php

namespace BaoPham\DynamoDb\Parsers;

class ExpressionAttributeNames
{
    /**
     * @var array
     */
    protected $mapping;

    /**
     * @var string
     */
    protected $prefix;

    public function __construct($prefix = '#')
    {
        $this->reset();
        $this->prefix = $prefix;
    }

    public function set($name)
    {
        $this->mapping["{$this->prefix}{$name}"] = $name;
    }

    public function get($placeholder)
    {
        return $this->mapping[$placeholder];
    }

    public function all()
    {
        return $this->mapping;
    }

    public function reset()
    {
        $this->mapping = [];

        return $this;
    }
}