Contribution Guide
==================

Thank you for considering contributing to this library. Please make sure your code follows the PSR-2 coding standard and the PSR-4 autoloading standard before sending a pull request.

Test
----

> * DynamoDb local version: 2016-01-07_1.0
> * DynamoDb local schema for tests created by the [DynamoDb local shell](http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Tools.DynamoDBLocal.Shell.html) is located [here](local_schema.js)

Run the following commands:

```bash
$ java -Djava.library.path=./DynamoDBLocal_lib -jar local/DynamoDBLocal.jar --port 3000
# In a separate tab
$ ./vendor/bin/phpunit
```

or

```bash
composer --timeout=0 run local
# In a separate tab
composer run-script test
```
