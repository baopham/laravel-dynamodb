<?php

namespace BaoPham\DynamoDb;

use Illuminate\Support\Facades\App;

trait ModelTrait
{
    public static function boot()
    {
        parent::boot();

        $observer = static::getObserverClassName();

        static::observe(new $observer(
            App::make('BaoPham\DynamoDb\DynamoDbClientInterface')
        ));
    }

    public static function getObserverClassName()
    {
        return 'BaoPham\DynamoDb\ModelObserver';
    }

    public function getDynamoDbTableName()
    {
        return $this->getTable();
    }
}
