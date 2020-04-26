package com.atguigu.streaming

import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import net.ipip.ipdb.{City, CityInfo}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import scalikejdbc.{ConnectionPool, _}

/**
  * sparkStreaming精准消费一次数据
  * 地区分组求出每日新增的vip数量
  */
object VipIncrementAnalysis {

  private val sdf = new SimpleDateFormat("yyyy-MM-dd")

  //从文件中获取参数
  private val properties = new Properties()
  properties.load(this.getClass.getClassLoader.getResourceAsStream("configuration.properties"))

  // 使用静态ip资源库
  private val ipdb: City = new City(this.getClass.getClassLoader.getResource("ipipfree.ipdb").getPath)

  //获取设置连接池的参数
  private val driver: String = properties.getProperty("jdbc.driver.class")
  private val url: String = properties.getProperty("jdbc.url")
  private val user: String = properties.getProperty("jdbc.user")
  private val password: String = properties.getProperty("jdbc.password")

  //注册jdbc
  Class.forName(driver)

  //设置连接池
  ConnectionPool.singleton(url, user, password)


  def main(args: Array[String]): Unit = {

    //参数检测
    if (args.length != 1) {
      println("Attention : Please input checkpointpath")
      System.exit(1)
    }

    //通过参数传递checkpoint的地址
    val checkPointPath = args(0)

    //从checkpoint中获取ssc，没有则进行创建
    val ssc: StreamingContext = StreamingContext.getOrCreate(checkPointPath,
      () => {
        getVipIncrementByCountry(checkPointPath)
      }
    )

    ssc.start()
    ssc.awaitTermination()
  }


  //根据地区分组的vip的增量数据
  def getVipIncrementByCountry(checkPointPath: String): StreamingContext = {

    //获取到时间间隔
    val processingInterval: Long = properties.getProperty("processing.Interval").toLong
    //获取kafka的broker
    val brokers: String = properties.getProperty("kafka.broker.list")

    //创建环境
    val sparkConf: SparkConf = new SparkConf()
      .set("spark.streaming.stopGracefullyOnShutdown", "true")
      .setMaster("local[*]") //wjx 集群模式则不要设置
      .setAppName(this.getClass.getSimpleName)
    val ssc = new StreamingContext(sparkConf, Seconds(processingInterval))
    ssc.sparkContext.setLogLevel("ERROR")

    //设置kafka参数
    val kafkaParams: Map[String, String] = Map[String, String]("metadata.broker.list" -> brokers, "enable.auto.commit" -> "false")

    //获取offset
        val fromOffset: Map[TopicAndPartition, Long] = DB.readOnly { implicit session =>
          sql"select topic,part_id,offset from topic_offset"
            .map(rs =>
              TopicAndPartition(rs.string(1), rs.int(2)) -> rs.long(3)
            ).list.apply().toMap
        }

    DB.readOnly((session: DBSession) => {
      session.list("select topic,part_id,offset from topic_offset") {
        rs =>TopicAndPartition(rs.string(1), rs.int(2)) -> rs.long(3)
      }.toMap
    })


    //处理message的格式与泛型相同
    val messageHandler = (msg: MessageAndMetadata[String, String]) => (msg.topic + "-" + msg.partition, msg.message())
    //创建offset的数组，来接收每个分区的offset
    var offsetRanges: Array[OffsetRange] = Array.empty[OffsetRange]

    //创建直接对接kafka的流
    val messageDStream: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](ssc, kafkaParams, fromOffset, messageHandler)


    // 业务计算
    messageDStream.transform { rdd =>
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    }.filter { msg =>
      // 过滤非完成订单的数据，且验证数据合法性
      filterCompleteOrder(msg)
    }.map { msg =>
      // 数据转换，返回((2019-04-03,北京),1)格式的数据
      getDateAndArea(msg)
    }.updateStateByKey[Int] {
      // 更新状态变量
      (seq: Seq[Int], buffer: Option[Int]) => {
        val sum = buffer.getOrElse(0) + seq.sum
        Some(sum)
      }
    }.filter(state => {
      filterFunc(state)
    }).foreachRDD(rdd => {
      // 将最近两天的数据返回给Driver端
      // TODO 要做事务操作，因此不能在分布式环境下操作，需要拉到Driver端进行事务处理
      val resultTuple: Array[((String, String), Int)] = rdd.collect()

      DB.localTx(implicit session => {
        resultTuple.foreach(msg => {
          val dt: String = msg._1._1
          val province: String = msg._1._2
          val cnt: Int = msg._2

          //统计结果持久化到数据库中
          //          sql"""replace into vip_increment_analysis(province,cnt,dt) values (${province},${cnt},${dt})""".executeUpdate().apply()
          session.executeUpdate("replace into vip_increment_analysis(province,cnt,dt) values (?,?,?)", province, cnt, dt)
          println(msg)
        })

        for (o <- offsetRanges) {
          println(o.topic, o.partition, o.fromOffset, o.untilOffset)
          //          sql"""update topic_offset set offset = ${o.untilOffset} where topic = ${o.topic} and part_id = ${o.partition}""".update.apply()
          session.update(s"update topic_offset set offset = ${o.untilOffset} where topic = '${o.topic}' and part_id = ${o.partition}")
        }
      })
    })


    // 开启检查点
    ssc.checkpoint(checkPointPath)
    messageDStream.checkpoint(Seconds(processingInterval * 10))
    ssc
  }

  /**
    * 过滤
    *
    * @return
    */
  def filterFunc(state: ((String, String), Int)): Boolean = {
    val day = state._1._1
    val eventTime = sdf.parse(day).getTime
    // 获取当前系统时间缀
    val currentTime = System.currentTimeMillis()
    // 两者比较，保留两天内的
    if (currentTime - eventTime >= 172800000) {
      true
    } else {
      true
    }
  }

  /**
    * 过滤没有完成的订单
    *
    * @param msg
    * @return
    */
  def filterCompleteOrder(msg: (String, String)): Boolean = {
    val keys: Array[String] = msg._2.split("\t")
    //数据长度不为17则不合法
    if (keys.length == 17) {
      val eventType = keys(15)
      "completeOrder".equals(eventType)
    } else {
      false
    }
  }

  /**
    * 获取时间和地区
    *
    * @param msg
    */
  def getDateAndArea(msg: (String, String)): ((String, String), Int) = {
    val keys: Array[String] = msg._2.split("\t")
    //获取ip地址
    val ip: String = keys(8)

    //获取日志的时间
    val eventTime: Long = keys(16).toLong

    //将时间戳转换为对应的时间
    val date: String = sdf.format(new Date(eventTime * 1000))

    //根据ip获取省份的信息
    var regionName = "未知"
    val info: CityInfo = ipdb.findInfo(ip, "CN")
    if (info != null) {
      regionName = info.getRegionName
    }
    ((date, regionName), 1)
  }


}
