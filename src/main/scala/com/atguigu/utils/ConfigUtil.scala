package com.atguigu.utils

import java.io.InputStream
import java.util.{Properties, ResourceBundle}

object ConfigUtil {

  private val rb = ResourceBundle.getBundle("configuration.properties")

  /**
    * 使用ResourceBundle读取配置文件
    * @param configName
    * @return
    */
  def getConfigFromBundle(configName:String): String ={
    rb.getString(configName)
  }

  /**
    * 从指定的配置文件中读取相应的配置
    * @param configName
    */
  def getConfigFromResource(configName:String): String ={
    val stream: InputStream = Thread.currentThread().getContextClassLoader.getResourceAsStream("configuration.properties")
    val properties = new Properties()
    properties.load(stream)

    properties.getProperty(configName)

  }
}
