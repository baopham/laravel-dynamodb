var params = {
    TableName: 'test_model',
    KeySchema: [ // The type of of schema.  Must start with a HASH type, with an optional second RANGE.
        { // Required HASH type attribute
            AttributeName: 'id',
            KeyType: 'HASH',
        }
    ],
    AttributeDefinitions: [ // The names and types of all primary and index key attributes only
        {
            AttributeName: 'id',
            AttributeType: 'S', // (S | N | B) for string, number, binary
        },
        {
            AttributeName: 'count',
            AttributeType: 'N', // (S | N | B) for string, number, binary
        }
    ],
    ProvisionedThroughput: { // required provisioned throughput for the table
        ReadCapacityUnits: 1,
        WriteCapacityUnits: 1,
    },
    GlobalSecondaryIndexes: [ // optional (list of GlobalSecondaryIndex)
        {
            IndexName: 'count_index',
            KeySchema: [
                { // Required HASH type attribute
                    AttributeName: 'count',
                    KeyType: 'HASH',
                }
            ],
            Projection: { // attributes to project into the index
                ProjectionType: 'ALL', // (ALL | KEYS_ONLY | INCLUDE)
            },
            ProvisionedThroughput: { // throughput to provision to the index
                ReadCapacityUnits: 1,
                WriteCapacityUnits: 1,
            },
        },
        // ... more global secondary indexes ...
    ]
};
dynamodb.createTable(params, function(err, data) {
    if (err) print(err); // an error occurred
    else print(data); // successful response
});

// Table with composite keys:

params = {
    TableName: 'composite_test_model',
    KeySchema: [ // The type of of schema.  Must start with a HASH type, with an optional second RANGE.
        { // Required HASH type attribute
            AttributeName: 'id',
            KeyType: 'HASH',
        },
        {
            AttributeName: 'id2',
            KeyType: 'RANGE',
        }
    ],
    AttributeDefinitions: [ // The names and types of all primary and index key attributes only
        {
            AttributeName: 'id',
            AttributeType: 'S', // (S | N | B) for string, number, binary
        },
        {
            AttributeName: 'id2',
            AttributeType: 'S', // (S | N | B) for string, number, binary
        },
        {
            AttributeName: 'count',
            AttributeType: 'N', // (S | N | B) for string, number, binary
        }
    ],
    ProvisionedThroughput: { // required provisioned throughput for the table
        ReadCapacityUnits: 1,
        WriteCapacityUnits: 1,
    },
    GlobalSecondaryIndexes: [ // optional (list of GlobalSecondaryIndex)
        {
            IndexName: 'count_index',
            KeySchema: [
                { // Required HASH type attribute
                    AttributeName: 'count',
                    KeyType: 'HASH',
                }
            ],
            Projection: { // attributes to project into the index
                ProjectionType: 'ALL', // (ALL | KEYS_ONLY | INCLUDE)
            },
            ProvisionedThroughput: { // throughput to provision to the index
                ReadCapacityUnits: 1,
                WriteCapacityUnits: 1,
            },
        },
        // ... more global secondary indexes ...
    ]
};
dynamodb.createTable(params, function(err, data) {
    if (err) print(err); // an error occurred
    else print(data); // successful response
});