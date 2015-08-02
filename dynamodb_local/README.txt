README
========

For an overview of DynamoDB Local please refer to the documentation at http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Tools.DynamoDBLocal.html



Release Notes
-----------------------------

2015-07-16_1.0

  * Add support for DynamoDB Streams

Note the following difference in DynamoDBLocal:

  * Exception messages may differ from those returned by the service.

  * Shard creation behavior may differ from that of the service since Local does not support partitioning.



Running DynamoDB Local
---------------------------------------------------------------

java -Djava.library.path=./DynamoDBLocal_lib -jar DynamoDBLocal.jar [options]

For more information on available options, run with the -help option:

  java -Djava.library.path=./DynamoDBLocal_lib -jar DynamoDBLocal.jar -help
