<?php

namespace BaoPham\DynamoDb\ConditionAnalyzer;

class Index
{
    /**
     * @var string
     */
    public $name;

    /**
     * @var string
     */
    public $hash;

    /**
     * @var string
     */
    public $range;

    public function __construct($name, $hash, $range)
    {
        $this->name = $name;
        $this->hash = $hash;
        $this->range = $range;
    }

    public function isComposite()
    {
        return isset($this->hash) && isset($this->range);
    }

    public function columns()
    {
        $columns = [];

        if ($this->hash) {
            $columns[] = $this->hash;
        }

        if ($this->range) {
            $columns[] = $this->range;
        }

        return $columns;
    }
}
