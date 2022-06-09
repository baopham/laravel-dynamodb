<?php

namespace BaoPham\DynamoDb;

use Aws\DynamoDb\DynamoDbClient;
use Aws\DynamoDb\Marshaler;
use Aws\Sts\StsClient;
use Aws\Sts\Exception\StsException;
use Illuminate\Support\Arr;
use Illuminate\Support\Facades\Log;

class DynamoDbClientService implements DynamoDbClientInterface
{
    /**
     * @var array
     */
    protected $clients;

    /**
     * @var \Aws\DynamoDb\Marshaler
     */
    protected $marshaler;

    /**
     * @var \BaoPham\DynamoDb\EmptyAttributeFilter
     */
    protected $attributeFilter;

    public function __construct(Marshaler $marshaler, EmptyAttributeFilter $filter)
    {
        $this->marshaler = $marshaler;
        $this->attributeFilter = $filter;
        $this->clients = [];
    }

    /**
     * @return \Aws\DynamoDb\DynamoDbClient
     */
    public function getClient($connection = null)
    {
        $connection = $connection ?: config('dynamodb.default');

        if (isset($this->clients[$connection])) {
            return $this->clients[$connection];
        }

        $config = config("dynamodb.connections.$connection", []);
        $config['version'] = '2012-08-10';
        $config['debug'] = $this->getDebugOptions(Arr::get($config, 'debug'));

        if (array_key_exists('assume_role_arn', $config)) {
            try {
                $stsConfig = $config;
                $stsConfig['version'] = "2011-06-15";
                $stsClient = new StsClient($stsConfig);
                $result = $stsClient->AssumeRole([
                    'DurationSeconds' => 900,
                    'RoleArn' => $config['assume_role_arn'],
                    'RoleSessionName' => config('app.name') . '-dynamodb',
                ]);
            } catch (StsException $e) {
                Log::error($e->getTraceAsString());
                return false;
            }

            $config['credentials'] = [
                'key' => $result['Credentials']['AccessKeyId'],
                'secret' => $result['Credentials']['SecretAccessKey'],
                'token' => $result['Credentials']['SessionToken'],
            ];
        }

        $client = new DynamoDbClient($config);

        $this->clients[$connection] = $client;

        return $client;
    }

    /**
     * @return \Aws\DynamoDb\Marshaler
     */
    public function getMarshaler()
    {
        return $this->marshaler;
    }

    /**
     * @return \BaoPham\DynamoDb\EmptyAttributeFilter
     */
    public function getAttributeFilter()
    {
        return $this->attributeFilter;
    }

    protected function getDebugOptions($debug = false)
    {
        if ($debug === true) {
            $logfn = function ($msg) {
                Log::info($msg);
            };

            return ['logfn' => $logfn];
        }

        return $debug;
    }
}
