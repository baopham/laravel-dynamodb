Contribution Guide
==================

Thank you for considering contributing to this library. Please make sure your code follows the PSR-2 coding standard and the PSR-4 autoloading standard before sending a pull request.

Test
----

Starting DynamoDB Local:

```bash
$ java -Djava.library.path=./DynamoDBLocal_lib -jar dynamodb_local/DynamoDBLocal.jar --port 3000
$ ./vendor/bin/phpunit
```

or

```bash
composer --timeout=0 run dynamodb_local
```

* DynamoDb local version: 2016-01-07_1.0

* DynamoDb local schema for tests created by the [DynamoDb local shell](http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Tools.DynamoDBLocal.Shell.html) is located [here](dynamodb_local_schema.js)

Running PHPUnit:

```bash
./vendor/bin/phpunit
```

or

```bash
composer run-script test
```