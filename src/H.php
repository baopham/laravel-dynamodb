<?php

namespace BaoPham\DynamoDb;

/**
 * Class H
 *
 * Short for "Helper".
 * We often get breaking changes from Laravel Helpers, so to ensure this won't happen again, we port the helpers here.
 *
 * @package BaoPham\DynamoDb
 */
// phpcs:disable PSR1.Methods.CamelCapsMethodName.NotCamelCaps
class H
{
    public static function array_first($array, callable $callback = null, $default = null)
    {
        if (is_null($callback)) {
            if (empty($array)) {
                return static::value($default);
            }
            foreach ($array as $item) {
                return $item;
            }
        }
        foreach ($array as $key => $value) {
            if (call_user_func($callback, $value, $key)) {
                return $value;
            }
        }
        return static::value($default);
    }

    public static function value($value)
    {
        return $value instanceof \Closure ? $value() : $value;
    }
}
// phpcs:enable Squiz.Classes.ValidClassName.NotCamelCaps
