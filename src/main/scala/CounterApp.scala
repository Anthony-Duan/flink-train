import org.apache.flink.api.common.accumulators.LongCounter
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala.{ExecutionEnvironment, _}
import org.apache.flink.configuration.Configuration
import org.apache.flink.core.fs.FileSystem.WriteMode

/**
  * @ Description:
  * @ Date: Created in 12:41 2019-03-24
  * @ Author: Anthony_Duan
  *
  *
  * * 基于Flink编程的计数器开发三步曲
  * * step1：定义计数器
  * * step2: 注册计数器
  * * step3: 获取计数器
  **/
object CounterApp {

  def main(args: Array[String]): Unit = {


    val env = ExecutionEnvironment.getExecutionEnvironment

    val data = env.fromElements("hadoop", "flink", "hadoop", "pyspark")

    val info = data.map(new RichMapFunction[String, String] {

      val counter = new LongCounter()

      override def open(parameters: Configuration): Unit = {
        getRuntimeContext.addAccumulator("myCounter", counter)
      }

      override def map(value: String): String = {
        counter.add(1)
        value
      }
    })

    val filePath = "file:///Users/duanjiaxing/data/myCounter/"

    info.writeAsText(filePath, WriteMode.OVERWRITE).setParallelism(3)
    val jobRe = env.execute("CounterApp")
    val re = jobRe.getAccumulatorResult[Long]("myCounter")
    println(re)

  }

}
