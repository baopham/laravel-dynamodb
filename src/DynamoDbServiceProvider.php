<?php

namespace BaoPham\DynamoDb;

use Aws\DynamoDb\Marshaler;
use Illuminate\Support\Facades\App;
use Illuminate\Support\Facades\Log;
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
    }

    /**
     * Register the service provider.
     */
    public function register()
    {
        $marshalerOptions = [
            'nullify_invalid' => true,
        ];

        if ($this->app->environment() == 'testing' || config('services.dynamodb.local')) {
            return $this->bindForTesting($marshalerOptions);
        }

        $this->bindForApp($marshalerOptions);
    }

    protected function getDebugOptions()
    {
        $debug = config('services.dynamodb.debug');

        if ($debug === true) {
            $logfn = function ($msg) {
                Log::info($msg);
            };

            return ['logfn' => $logfn];
        }

        return $debug;
    }

    protected function bindForApp($marshalerOptions = [])
    {
        $this->app->singleton('BaoPham\DynamoDb\DynamoDbClientInterface', function ($app) use ($marshalerOptions) {
            $config = [
                'credentials' => [
                    'key' => config('services.dynamodb.key'),
                    'secret' => config('services.dynamodb.secret'),
                    'token' => config('services.dynamodb.token'),
                ],
                'region' => config('services.dynamodb.region'),
                'version' => '2012-08-10',
                'debug' => $this->getDebugOptions(),
            ];

            $client = new DynamoDbClientService($config, new Marshaler($marshalerOptions), new EmptyAttributeFilter());

            return $client;
        });
    }

    protected function bindForTesting($marshalerOptions = [])
    {
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
                'debug' => $this->getDebugOptions(),
            ];

            $client = new DynamoDbClientService($config, new Marshaler($marshalerOptions), new EmptyAttributeFilter());

            return $client;
        });
    }
}
