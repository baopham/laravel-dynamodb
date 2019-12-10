<?php

namespace Rennokki\DynamoDb;

/**
 * Class RawDynamoDbQuery.
 */
class RawDynamoDbQuery implements \IteratorAggregate, \ArrayAccess, \Countable
{
    /**
     * 'Scan', 'Query', etc.
     *
     * @var string
     */
    public $op;

    /**
     * The query body being sent to AWS.
     *
     * @var array
     */
    public $query;

    public function __construct($op, $query)
    {
        $this->op = $op;
        $this->query = $query;
    }

    /**
     * Perform any final clean up.
     * Remove any empty values to avoid errors.
     *
     * @return $this
     */
    public function finalize()
    {
        $this->query = array_filter($this->query, function ($value) {
            return ! empty($value) || is_bool($value) || is_numeric($value);
        });

        return $this;
    }

    /**
     * Whether a offset exists.
     * @link http://php.net/manual/en/arrayaccess.offsetexists.php
     * @param mixed $offset <p>
     * An offset to check for.
     * </p>
     * @return bool true on success or false on failure.
     * </p>
     * <p>
     * The return value will be casted to boolean if non-boolean was returned.
     * @since 5.0.0
     */
    public function offsetExists($offset)
    {
        return isset($this->internal()[$offset]);
    }

    /**
     * Offset to retrieve.
     * @link http://php.net/manual/en/arrayaccess.offsetget.php
     * @param mixed $offset <p>
     * The offset to retrieve.
     * </p>
     * @return mixed Can return all value types.
     * @since 5.0.0
     */
    public function offsetGet($offset)
    {
        return $this->internal()[$offset];
    }

    /**
     * Offset to set.
     * @link http://php.net/manual/en/arrayaccess.offsetset.php
     * @param mixed $offset <p>
     * The offset to assign the value to.
     * </p>
     * @param mixed $value <p>
     * The value to set.
     * </p>
     * @return void
     * @since 5.0.0
     */
    public function offsetSet($offset, $value)
    {
        $this->internal()[$offset] = $value;
    }

    /**
     * Offset to unset.
     * @link http://php.net/manual/en/arrayaccess.offsetunset.php
     * @param mixed $offset <p>
     * The offset to unset.
     * </p>
     * @return void
     * @since 5.0.0
     */
    public function offsetUnset($offset)
    {
        unset($this->internal()[$offset]);
    }

    /**
     * Retrieve an external iterator.
     * @link http://php.net/manual/en/iteratoraggregate.getiterator.php
     * @return \Traversable An instance of an object implementing <b>Iterator</b> or
     * <b>Traversable</b>
     * @since 5.0.0
     */
    public function getIterator()
    {
        return new \ArrayObject($this->internal());
    }

    /**
     * Count elements of an object.
     * @link http://php.net/manual/en/countable.count.php
     * @return int The custom count as an integer.
     * </p>
     * <p>
     * The return value is cast to an integer.
     * @since 5.1.0
     */
    public function count()
    {
        return count($this->internal());
    }

    /**
     * For backward compatibility, previously we use array to represent the raw query.
     *
     * @var array
     *
     * @return array
     */
    private function internal()
    {
        return [$this->op, $this->query];
    }
}
