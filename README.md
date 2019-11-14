<h4>Concurrent Queuing with Kafka, Spring Boot and Simulated Data Records</h4>
<ul>
<li>Kafka partitions are limited by Zookeeper znodes. There's at least one Apache ZooKeeper znode per topic in Kafka. The max number of znodes realistically supported is roughly 10k znodes.</li>
<li>Alternative to creating a topic per queue would be to use the key in the Kafka Record through a Concurrent HashMap to support a high volume queuing problem where the number of queues is greater than the znode limit.</li>
<li>Spring Boot framework is used so that test application can be readily extended</li>
<li>Kafka Producer and Consumer are used outside of Spring Framework to maintain compatibility with latest Kafka version</li>
<li>Json Marshal/UnMarshall is also done with Jackson Mapper rather than use Spring Boot JsonSerializer/JsonDeserializer as there have been changes in recent Spring Boot versions. By using Jackson Mapper directly, there are no incompatibility issues. </li>
<li>Concurrent Maps are used with BlockingQueues that have limits (4096) to apply back pressure against the Kafka Consumer if needed</li>
<li>A TreeMap is used at the last stage to sort the queue totals processed from each queue.</li>
</ul>
