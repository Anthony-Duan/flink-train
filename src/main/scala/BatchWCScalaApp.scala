import org.apache.flink.api.scala.ExecutionEnvironment

/**
  * @ Description:
  * @ Date: Created in 16:02 2019-03-23
  * @ Author: Anthony_Duan
  */
object BatchWCScalaApp {

  def main(args: Array[String]): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment

    val input = "file:///Users/duanjiaxing/data/hello.txt"

    val text = env.readTextFile(input)

    import org.apache.flink.api.scala._

    text.flatMap(_.toLowerCase().split("\t"))
      .filter(_.nonEmpty)
      .map((_, 1))
      .groupBy(0)
      .sum(1)
      .print()
  }



}
