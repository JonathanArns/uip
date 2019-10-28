# A Flink example project using Java and Gradle.

To package your job for submission to Flink, use: ``gradle clean shadowJar``. Afterwards, you'll find the
jar to use in the 'build/libs' folder.

Make sure to use Java 8 as the Project-SDK.

To start Kafka and a Flink cluster:
```
docker-compose up
```
Create the Kafka topics:
```
docker exec kafka kafka-topics --create --topic flink_input --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
docker exec kafka kafka-topics --create --topic flink_output --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```
Open the console Kafka producer:
```
docker exec -it kafka kafka-console-producer --broker-list localhost:9092 --topic flink_input
```
Open the console Kafka consumer:
```
docker exec -it kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic flink_output --from-beginning
```
On ``localhost:8081`` you can find the flink web-gui, here you can submit your job (the .jar file). Now if you write something in the console producer you will see flink's output in the console consumer.
