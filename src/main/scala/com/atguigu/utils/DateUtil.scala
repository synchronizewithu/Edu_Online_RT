package scala.com.atguigu.utils

import java.text.SimpleDateFormat
import java.util.Date

object DateUtil {

  /**
    * 解析事件long的时间戳
    * @param timestamp
    * @return
    */
  def parseTime(timestamp:Long,pattern:String):String={
    val sdf = new SimpleDateFormat(pattern)
    sdf.format(new Date(timestamp))
  }

}
