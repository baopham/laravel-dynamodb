<?php

namespace Rennokki\DynamoDb\Tests;

use PHPUnit\Framework\TestCase;

/**
 * Class EmptyAttributeFilterTest.
 */
class EmptyAttributeFilterTest extends TestCase
{
    public function testEmptyAttributeFilter()
    {
        $filter = new \Rennokki\DynamoDb\EmptyAttributeFilter();

        $attributes = [
            'attr1' => [],
            'attr2' => [
                'attr2.1' => '',
                'attr2.2' => ' ',
                'attr2.3' => null,
                'attr2.4' => 'foo',
                'attr2.5' => [
                    'foobar' => 'foobar',
                    'foobar2' => false,
                    'foobar3' => '',
                    'foobar4' => [
                        'foobar4.1' => [],
                    ],
                ],
            ],
        ];

        $expected = [
            'attr1' => null,
            'attr2' => [
                'attr2.1' => null,
                'attr2.2' => null,
                'attr2.3' => null,
                'attr2.4' => 'foo',
                'attr2.5' => [
                    'foobar' => 'foobar',
                    'foobar2' => false,
                    'foobar3' => null,
                    'foobar4' => [
                        'foobar4.1' => null,
                    ],
                ],
            ],
        ];

        $filter->filter($attributes);

        $this->assertEquals($expected, $attributes);
    }
}
