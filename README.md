# flume.kafka
A Flume kafka sink based on new Kafka Producer, high performance and configurable. It depends very fewer projects/libs,only Flume 1.5.2
kafka-clients-0.8.2.1 or later,slf4j.
Similar to Flume 1.6 KafkaSink, but there are some differences here:
<li> Flume 1.6 is not based on new Produce Config
<li> KafkaProducer is thread-safe, so can use the same producer if KafkaProducer Config is same.
<li> add Callback for KafkaProducer method, so can deal with the situation send failed immediately,and get more debug info.
<li> send event one bye one other than taken all events from channel,and then send all of them, because I think it will cost a lot of time when 
the event batch size is huge(Sorry, I have enough resource to test this situation ).

<pre>
目前，只是实现KafkaSink。实现参照了Flume 1.6 的KafkaSink<br/>
1. 基于kafka-clients-0.8.2.1即new Kafka Producer实现，整合的Flume版本为1.5.2<br/>
2. 类似于Flume 1.6（尚未正式发布）的KafkaSink，比如:
  A. topic选择的顺序,按如下优先级
    a.从event的header中提取topic，如果有且不为空，使用该topic
    b.从配置文件中读取固定topic
  B. 支持批量发送，event batch大小可以设置
  C. 增加metrics
3. 增加了metrics，因此扩展了SinkCounter类，这个在Flume 1.6中也已经修改了SinkCounter的构造函数
4. 不同于Flume 1.6之处：
  A. Flume 1.6 不是基于最新的Producer配置 new Producer.
  B. KafkaProducer是一个线程安全thread-safe的类，因此，一个agent如果配置文件相同，只需要一个KafkaProducer实例。
  C. KafkaProducer的send方法是一个异步asynchronous的. Flume 1.6中，是没有基于回调的，因此不能对每一次数据发送进行处理，如日志和测量
  注：但因为批量处理，因此，没有使用Future，JAVA的Future中的get方法是阻塞blocking的，虽然可基于GUAVA来实现非阻塞unblocking 的Future.
  D. Flume 1.6中，是先通过channel中获取所有event，然后批量发送，但当批量大小很大时，从Channel中读取event本身就比较耗时。因此， 
  这里采用每收到一个event就立即发送
  E. Flume 1.6中，目前还有这个bug（JIRA中已经提出来了），当从Channel获取不到event（即event = null）时，应该使用Status.BACKOFF
  而不是Status.READY。
  </pre>
