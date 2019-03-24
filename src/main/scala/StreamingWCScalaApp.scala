import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * @ Description:
  * @ Date: Created in 16:32 2019-03-23
  * @ Author: Anthony_Duan
  */
object StreamingWCScalaApp {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    StreamExecutionEnvironment.createLocalEnvironment()

    import org.apache.flink.api.scala._
    val text = env.socketTextStream("localhost",9999)
    text.flatMap(_.split(","))
//      .map((_,1))
      .map(x=>WC(x.toLowerCase(),1)) //WC(word,count)
//      .keyBy(0)
      .keyBy("word")
      .timeWindow(Time.seconds(3))
//      .sum(1)
      .sum("count")
      .setParallelism(1)
      .print()

    env.execute("StreamingWCScalaApp")

    case class WC(word:String,count:Int)
  }

}
