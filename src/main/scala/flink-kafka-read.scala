import org.apache.flink.streaming.api.scala._
import java.util.Properties
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.streaming.util.serialization.SimpleStringSchema
/**
 * Created by tyang on 18/09/15.
 */
object ReadFromKafka {
  def main(args: Array[String]) {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //Get the Execution Environment
    val props = new Properties
    props.setProperty("zookeeper.connect","localhost:2181")
    props.setProperty("bootstrap.servers","localhost:9092")
    props.setProperty("group.id","test_group")
    //Configure properties of zookeeper server address and broker address, as well as group ID
    val MyFlinkKafkaConsumer = new FlinkKafkaConsumer[String]("test",new SimpleStringSchema, props, FlinkKafkaConsumer.OffsetStore.FLINK_ZOOKEEPER, FlinkKafkaConsumer.FetcherType.LEGACY_LOW_LEVEL)
    //Create a new FlinkKafkaConsumer for the program
    val stream = env.addSource(MyFlinkKafkaConsumer)
    //Open a data stream in ExecutionEnvironment
    stream.rebalance.map("ReadFromKafka : " + _).print
    println("Hi Kafka")
    env.execute //Execute the data stream
    println("It works!")
  }
}