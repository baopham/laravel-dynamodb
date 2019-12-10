<?php

namespace Rennokki\DynamoDb;

use Illuminate\Support\Facades\App;

trait ModelTrait
{
    public static function boot()
    {
        parent::boot();

        $observer = static::getObserverClassName();

        static::observe(new $observer(
            App::make('Rennokki\DynamoDb\DynamoDbClientInterface')
        ));
    }

    public static function getObserverClassName()
    {
        return 'Rennokki\DynamoDb\ModelObserver';
    }

    public function getDynamoDbTableName()
    {
        return $this->getTable();
    }
}
