<?php

namespace BaoPham\DynamoDb\Parsers;

class ExpressionAttributeNames
{
    /**
     * @var array
     */
    protected $mapping;

    /**
     * @var array
     */
    protected $nested;

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
        if ($this->isNested($name)) {
            $this->nested[] = $name;
            return;
        }
        $this->mapping["{$this->prefix}{$name}"] = $name;
    }

    public function get($placeholder)
    {
        return $this->mapping[$placeholder];
    }

    public function placeholder($name)
    {
        $placeholder = "{$this->prefix}{$name}";
        if (isset($this->mapping[$placeholder])) {
            return $placeholder;
        }
        return $name;
    }

    public function all()
    {
        return $this->mapping;
    }

    public function placeholders()
    {
        return array_merge(array_keys($this->mapping), $this->nested);
    }

    public function reset()
    {
        $this->mapping = [];
        $this->nested = [];

        return $this;
    }

    private function isNested($name)
    {
        return strpos($name, '.') !== false || (strpos($name, '[') !== false && strpos($name, ']') !== false);
    }
}
