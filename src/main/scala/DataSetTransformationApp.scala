import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.scala.{ExecutionEnvironment, _}

import scala.collection.mutable.ListBuffer

/**
  * @ Description:
  * @ Date: Created in 10:19 2019-03-24
  * @ Author: Anthony_Duan
  */
object DataSetTransformationApp {


  def mapFunction(env: ExecutionEnvironment): Unit = {
    val data = env.fromCollection(List(1, 2, 3, 4, 5, 6, 7))

    data.map(_ + 1).print()
  }

  def filterFunction(env: ExecutionEnvironment) = {
    /**
      * Creates a DataSet from the given non-empty [[Iterable]].
      *
      * Note that this operation will result in a non-parallel data source, i.e. a data source with
      * a parallelism of one.
      */
    //    env.fromCollection(List(1,2,3,4,5))
    //      .map(_+1)
    //      .filter(_>4)
    //      .print()


    /**
      * Creates a new data set that contains the given elements.
      * *
      * * * Note that this operation will result in a non-parallel data source, i.e. a data source with
      * * a parallelism of one.
      */
    env.fromElements(1, 2, 3, 4, 5)
      .map(_ + 1)
      .filter(_ > 4)
      .print()
  }


  def mapPartitionFunction(env: ExecutionEnvironment) = {
    val students = new ListBuffer[String]
    for (i <- 1 to 10) {
      students.append("student: " + i)
    }

    //设置数据分区
    val data = env.fromCollection(students).setParallelism(5)

    //按照分区进行map，数据库的分区写入操作
    data.mapPartition(x => {
      val connection = DBUtils.getConnection()
      println(s"connection$connection...")
      //TODO... 保存数据到DB
      DBUtils.returnConnection(connection)

      x
    }).print()
  }


  def firstFunction(env: ExecutionEnvironment): Unit = {
    val info = ListBuffer[(Int, String)]()
    info.append((1, "Hadoop"))
    info.append((1, "Spark"))
    info.append((1, "Flink"))
    info.append((2, "Java"))
    info.append((2, "Spring Boot"))
    info.append((3, "Linux"))
    info.append((4, "VUE"))
    info.appendAll(List((1, "Hadoop"), (2, "Hadoop")))
    info.foreach(println(_))
    println()
    val data = env.fromCollection(info)
    data.groupBy(0)
      .sortGroup(1, Order.DESCENDING)
      .first(2)
      .print()

  }


  def flatMapFunction(env: ExecutionEnvironment): Unit = {
    val info = ListBuffer[String]()
    info.appendAll(List("hadoop,spark", "hadoop,flink", "flink,flink"))
    val data = env.fromCollection(info)

    data.flatMap(_.split(",").map((_, 1)))
      .groupBy(0)
      .sum(1)
      //      .map(x=>x._1+": "+x._2)
      //      最外层一定要有action算子，内部的action算子不能被识别，
      //      如果没有就不会进行计算
      //      .map(t=>println(t))
      .print()

  }


  def joinFunction(env: ExecutionEnvironment): Unit = {
    val info1 = ListBuffer[(Int, String)]()
    val info2 = ListBuffer[(Int, String)]()

    info1.appendAll(List((1, "zs"), (2, "ls"), (3, "ww"), (4, "ml")))
    info2.appendAll(List((2, "北京"), (3, "杭州"), (1, "成都"), (4, "南京")))


    val data1 = env.fromCollection(info1)
    val data2 = env.fromCollection(info2)

    data1.join(data2).where(0).equalTo(0) {
      (first, second) => (first._1, first._2, second._2)
    }.print()
  }

  def crossFunction(env: ExecutionEnvironment): Unit = {
    val info1 = List("成都", "北京")
    val info2 = List(1, 2, 3)
    val data1 = env.fromCollection(info1)
    val data2 = env.fromCollection(info2)

    data1.cross(data2).print()

  }

  def main(args: Array[String]): Unit = {
    import org.apache.flink.api.scala._

    val env = ExecutionEnvironment.getExecutionEnvironment

    //    mapFunction(env)
    //    filterFunction(env)

    //    mapPartitionFunction(env)
    //    firstFunction(env)

    //    flatMapFunction(env)
    //    joinFunction(env)
    crossFunction(env)
  }

}
