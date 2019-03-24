
import scala.util.Random

/**
  * @ Description:
  * @ Date: Created in 10:46 2019-03-24
  * @ Author: Anthony_Duan
  */
object DBUtils {

  def getConnection() = {
    new Random().nextInt(10) + ""
  }

  def returnConnection(connection: String): Unit = {

  }

}
