<?php

namespace BaoPham\DynamoDb;

use Aws\DynamoDb\Marshaler;
use Illuminate\Support\Facades\App;
use Illuminate\Support\ServiceProvider;

class DynamoDbServiceProvider extends ServiceProvider
{

    /**
     * Register the service provider.
     *
     * @return void
     */
    public function register()
    {
        $marshalerOptions = [
            'nullify_invalid' => true,
        ];

        $this->app->singleton('BaoPham\DynamoDb\DynamoDbClientInterface', function ($app) use ($marshalerOptions) {
            $service_config = config('services.dynamodb');
            $config = array();

            foreach ($service_config as $name => $named_config) {
                if (App::environment() == 'testing' || $named_config['local']) {
                    $region = App::environment() == 'testing' ? 'test' : 'stub';
                } else {
                    $region = $named_config['region'];
                }

                $config[$name] = [
                    'credentials' => [
                        'key' => $named_config['key'],
                        'secret' => $named_config['secret'],
                    ],
                    'region' => $region,
                    'version' => '2012-08-10',
                ];

                if ($named_config['local']) {
                    $config[$name]['endpoint'] = $named_config['local_endpoint'];
                }
            }

            $client = new DynamoDbClientService($config, new Marshaler($marshalerOptions), new EmptyAttributeFilter);
            return $client;
        });
    }
}
