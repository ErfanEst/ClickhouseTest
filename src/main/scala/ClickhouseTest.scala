import core.Core.{appConfig, spark}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, to_date, to_timestamp, udf}
import utils.Utils.CommonColumns.{bibID, month_index}
import utils.Utils.monthIndexOf
import org.yaml.snakeyaml.Yaml
import java.io.InputStream
import java.sql.DriverManager
import scala.jdk.CollectionConverters._
import scala.util.Using

object ClickhouseTest {

  def main(args: Array[String]): Unit = {
    val tables = readTablesFromYAML("tables.yml")

    val url = "jdbc:clickhouse://localhost:8123/default"
    val user = "default"
    val password = "291177"

    Using.Manager { use =>
      val connection = use(DriverManager.getConnection(url, user, password))
      val stmt = use(connection.createStatement())
      val rs = use(stmt.executeQuery("SELECT version()"))

      if (rs.next()) {
        println(s"✅ ClickHouse Version: ${rs.getString(1)}")
      } else {
        println("⚠️ No data returned from ClickHouse")
      }
    }.recover {
      case ex => println(s"❌ Failed to connect to ClickHouse: ${ex.getMessage}")
    }

    tables.foreach(createClickhouseTable)

    val index = 16846
    try {
      val rechargeDF = readRecharge("recharge", index)
      println("✅ Successfully read recharge data:")
      rechargeDF.show(5, truncate = false)
      rechargeDF.printSchema()

      bulkInsertToClickhouse(rechargeDF, "recharge")
    } catch {
      case ex: Exception =>
        println(s"❌ Error reading recharge data: ${ex.getMessage}")
    }
  }

  case class ColumnDef(name: String, `type`: String)
  case class Table(name: String, columns: List[ColumnDef], order_by: List[String])


  def readTablesFromYAML(resourcePath: String): List[Table] = {
    val yaml = new Yaml()
    val inputStream: InputStream = getClass.getClassLoader.getResourceAsStream(resourcePath)
    if (inputStream == null) {
      throw new RuntimeException(s"Resource file not found: $resourcePath")
    }

    val parsedData = yaml.load(inputStream).asInstanceOf[java.util.Map[String, Any]]
    val parquetTables = parsedData.get("tables").asInstanceOf[java.util.List[java.util.Map[String, Any]]]

    parquetTables.asScala.map { tableMap =>
      val name = tableMap.get("name").asInstanceOf[String]
      val columnsList = tableMap.get("columns").asInstanceOf[java.util.List[java.util.Map[String, String]]]

      val columns = columnsList.asScala.map { colMap =>
        ColumnDef(colMap.get("name"), colMap.get("type"))
      }.toList

      val orderByList = tableMap.get("order_by") match {
        case list: java.util.List[_] => list.asScala.toList.map(_.toString)
        case _ => List(columns.head.name) // fallback اگر تو YAML نبود
      }

      Table(name, columns, orderByList)

    }.toList
  }

  def createClickhouseTable(table: Table): Unit = {
    val connection = DriverManager.getConnection("jdbc:clickhouse://localhost:8123/default", "default", "291177")
    try {
      val stmt = connection.createStatement()

      val rs = stmt.executeQuery(s"EXISTS TABLE ${table.name}")
      val tableExists = if (rs.next()) rs.getBoolean(1) else false

      if (!tableExists) {
        val columnsSql = table.columns.map(col => s"${col.name} ${col.`type`}").mkString(", ")
        val orderBySql = table.order_by.mkString(", ")

        val createTableQuery =
          s"""
             |CREATE TABLE ${table.name} (
             |  $columnsSql
             |) ENGINE = MergeTree()
             |ORDER BY ($orderBySql)
             |""".stripMargin

        println(s"📜 Generated SQL for '${table.name}':\n$createTableQuery")

        stmt.execute(createTableQuery)
        println(s"✅ Table '${table.name}' created successfully.")
      } else {
        println(s"ℹ️ Table '${table.name}' already exists.")
      }
    } catch {
      case ex: Exception =>
        println(s"❌ Failed to create/check table '${table.name}': ${ex.getMessage}")
    } finally {
      connection.close()
    }
  }

  def readRecharge(fileType: String, index: Int): DataFrame = {
    fileType match {
      case "recharge" =>
        val monthIndexOfUDF = udf((date: String) => monthIndexOf(date))

        spark.read.parquet(appConfig.getString("Path.Recharge"))
          .filter(col(bibID).isNotNull)
          .withColumn("recharge_dt", to_timestamp(col("recharge_dt"), "yyyyMMdd' 'HH:mm:ss"))
          .withColumn("date", to_date(col("date_key"), "yyyyMMdd"))
          .withColumn(month_index, monthIndexOfUDF(col("date")))
          .drop("date_key")
          .repartition(300)

      case other =>
        throw new IllegalArgumentException(s"Unknown file type: $other")
    }
  }

  def bulkInsertToClickhouse(df: DataFrame, tableName: String): Unit = {
    df.write
      .format("jdbc")
      .option("url", "jdbc:clickhouse://localhost:8123/default")
      .option("dbtable", tableName)
      .option("user", "default")
      .option("password", "291177")
      .mode("append")
      .save()

    println(s"✅ Data inserted successfully into ClickHouse table '$tableName'.")
  }
}
