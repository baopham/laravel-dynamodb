README
========

For an overview of DynamoDB Local please refer to the documentation at http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Tools.DynamoDBLocal.html



Release Notes
-----------------------------

2016-01-07_1.0

  * Bug Fixes for Online Index Item Violation

  * Process Exit on SocketException


Running DynamoDB Local
---------------------------------------------------------------

java -Djava.library.path=./DynamoDBLocal_lib -jar DynamoDBLocal.jar [options]

For more information on available options, run with the -help option:

  java -Djava.library.path=./DynamoDBLocal_lib -jar DynamoDBLocal.jar -help
