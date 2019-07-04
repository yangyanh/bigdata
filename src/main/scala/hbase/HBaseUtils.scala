package hbase

import org.apache.hadoop.hbase.{HBaseConfiguration, HTableDescriptor, TableName}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{ConnectionFactory, HBaseAdmin, HTable}
import org.apache.hadoop.hbase.mapreduce.TableInputFormat

object HBaseUtils {

  val quorum = "193.112.101.225"
  val port = "2181"

  /**
    * 设置HBaseConfiguration
    * @param tableName
    */
  def getHBaseConfiguration(tableName:String) = {
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum",quorum)
    conf.set("hbase.zookeeper.property.clientPort",port)
    conf.set(TableInputFormat.INPUT_TABLE,tableName)
    conf
  }

  /**
    * 返回或新建HBaseAdmin
    * @param conf
    * @param tableName
    * @return
    */
  def getHBaseAdmin(conf:Configuration,tableName:String) = {
    val admin = new HBaseAdmin(conf)
    if (!admin.isTableAvailable(tableName)) {
      val tableDesc = new HTableDescriptor(TableName.valueOf(tableName))
      admin.createTable(tableDesc)
    }

    admin
  }

  /**
    * 返回HTable
    * @param conf
    * @param tableName
    * @return
    */
  def getTable(conf:Configuration,tableName:String) = {
    println("begin connect")
    val conn = ConnectionFactory.createConnection(conf)
    println("end connect get table")
    val StatTable = conn.getTable(TableName.valueOf(tableName))
    println("get stable :"+StatTable.toString)
    StatTable
//    new HTable(conf,tableName)
  }
}
