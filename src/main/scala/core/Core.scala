package core

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.SparkSession


object Core {

  val appConfig: Config = ConfigFactory.load
  lazy val spark: SparkSession = SparkSession.builder
    .appName(appConfig.getString("spark.appName"))
    .master(appConfig.getString("spark.master")) // Use all cores to match your system capacity while leaving 2 cores free for the OS
    .config("spark.executor.memory", appConfig.getString("spark.executorMemory")) // 16GB memory for each executor
    .config("spark.driver.memory", appConfig.getString("spark.driverMemory")) // 16GB memory for the driver
    .getOrCreate

}
