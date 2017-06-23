# Contribution Guide

Thank you for considering contributing to this library. Please make sure your code follows the PSR-2 coding standard and the PSR-4 autoloading standard before sending a pull request.

## Running Tests

Starting DynamoDB Local
```java -Djava.library.path=./DynamoDBLocal_lib -jar dynamodb_local/DynamoDBLocal.jar --port 3000```

or

```composer --timeout=0 run dynamodb_local```

Running Tests
```./vendor/bin/phpunit```

or

```composer run-script test```