import org.apache.commons.io.FileUtils
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala.{ExecutionEnvironment, _}
import org.apache.flink.configuration.Configuration

/**
  * @ Description:
  * @ Date: Created in 12:13 2019-03-24
  * @ Author: Anthony_Duan
  */
object DistributedCacheApp {


  def main(args: Array[String]): Unit = {


    val env = ExecutionEnvironment.getExecutionEnvironment

    val filepath = "file:///Users/duanjiaxing/data/hello.txt"


    env.registerCachedFile(filepath, "myFile")
    val data = env.fromElements("hadoop", "spark", "flink")


    data.map(new RichMapFunction[String, String] {
      override def open(parameters: Configuration): Unit = {
        val myFile = getRuntimeContext.getDistributedCache.getFile("myFile")
        val lines = FileUtils.readLines(myFile)

        //scala遍历java集合
        import scala.collection.JavaConverters._

        for (ele <- lines.asScala) {
          println(ele)
        }
      }

      override def map(value: String): String = value
    }).print()
  }
}
