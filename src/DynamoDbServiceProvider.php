<?php

namespace BaoPham\DynamoDb;

use Aws\DynamoDb\Marshaler;
use BaoPham\DynamoDb\DynamoDb\DynamoDbManager;
use Illuminate\Support\ServiceProvider;

class DynamoDbServiceProvider extends ServiceProvider
{
    /**
     * Bootstrap the application events.
     *
     * @return void
     */
    public function boot()
    {
        DynamoDbModel::setDynamoDbClientService($this->app->make(DynamoDbClientInterface::class));

        $this->publishes([
            __DIR__.'/../config/dynamodb.php' => app()->basePath('config/dynamodb.php'),
        ]);
    }

    /**
     * Register the service provider.
     */
    public function register()
    {
        $marshalerOptions = [
            'nullify_invalid' => true,
        ];

        $this->app->singleton(DynamoDbClientInterface::class, function () use ($marshalerOptions) {
            $client = new DynamoDbClientService(new Marshaler($marshalerOptions), new EmptyAttributeFilter());

            return $client;
        });

        $this->app->singleton('dynamodb', function () {
            return new DynamoDbManager(app(DynamoDbClientInterface::class));
        });
    }
}
