<?php

return [
    'default' => env('DYNAMODB_CONNECTION', 'aws'),

    'connections' => [
        'aws' => [
            'credentials' => [
                'key' => env('DYNAMODB_KEY'),
                'secret' => env('DYNAMODB_SECRET'),
                'token' => env('AWS_SESSION_TOKEN'),
            ],
            'region' => env('DYNAMODB_REGION'),
            'local_endpoint' => null,
            'local' => false,
             // if true, it will use Laravel Log. For advanced options, see http://docs.aws.amazon.com/aws-sdk-php/v3/guide/guide/configuration.html
            'debug' => env('DYNAMODB_DEBUG'),
        ],
        'aws_iam_role' => [
            'region' => env('DYNAMODB_REGION'),
            'local_endpoint' => null,
            'local' => false,
            'debug' => true,
        ],
        'local' => [
            'credentials' => [
                'key' => 'dynamodb_local',
                'secret' => 'secret',
            ],
            'region' => 'stub',
             // see http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Tools.DynamoDBLocal.html
            'local_endpoint' => env('DYNAMODB_LOCAL_ENDPOINT'),
            'local' => true,
            'debug' => true,
        ],
        'test' => [
            'credentials' => [
                'key' => 'dynamodb_local',
                'secret' => 'secret',
            ],
            'region' => 'test',
            'local_endpoint' => env('DYNAMODB_LOCAL_ENDPOINT'),
            'local' => true,
            'debug' => true,
        ],
    ],
];
