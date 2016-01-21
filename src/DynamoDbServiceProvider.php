<?php

namespace BaoPham\DynamoDb;

use Aws\DynamoDb\Marshaler;
use Illuminate\Support\Facades\App;
use Illuminate\Support\ServiceProvider;

class DynamoDbServiceProvider extends ServiceProvider
{
    /**
     * Register the service provider.
     */
    public function register()
    {
        $marshalerOptions = [
            'nullify_invalid' => true,
        ];

        if (App::environment() == 'testing' || config('services.dynamodb.local')) {
            $this->app->singleton('BaoPham\DynamoDb\DynamoDbClientInterface', function ($app) use ($marshalerOptions) {
                $region = App::environment() == 'testing' ? 'test' : 'stub';

                $config = [
                    'credentials' => [
                        'key' => 'dynamodb_local',
                        'secret' => 'secret',
                    ],
                    'region' => $region,
                    'version' => '2012-08-10',
                    'endpoint' => config('services.dynamodb.local_endpoint'),
                ];
                $client = new DynamoDbClientService($config, new Marshaler($marshalerOptions), new EmptyAttributeFilter());

                return $client;
            });
        } else {
            $this->app->singleton('BaoPham\DynamoDb\DynamoDbClientInterface', function ($app) use ($marshalerOptions) {
                $config = [
                    'credentials' => [
                        'key' => config('services.dynamodb.key'),
                        'secret' => config('services.dynamodb.secret'),
                    ],
                    'region' => config('services.dynamodb.region'),
                    'version' => '2012-08-10',
                ];
                $client = new DynamoDbClientService($config, new Marshaler($marshalerOptions), new EmptyAttributeFilter());

                return $client;
            });
        }
    }
}
