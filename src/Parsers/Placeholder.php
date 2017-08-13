<?php

namespace BaoPham\DynamoDb\Parsers;

class Placeholder
{
    /**
     * @var int
     */
    private $counter;

    public function __construct()
    {
        $this->reset();
    }

    public function next()
    {
        $this->counter += 1;
        return "a{$this->counter}";
    }

    public function reset()
    {
        $this->counter = 0;

        return $this;
    }
}
