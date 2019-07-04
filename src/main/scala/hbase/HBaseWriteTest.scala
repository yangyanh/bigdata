package hbase

import org.apache.hadoop.hbase.client.{HTable, Put}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by guoxingyu on 2018/8/17.
  * 通过HTable中的Put向HBase写数据
  */

object HBaseWriteTest {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("HBaseWriteTest").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)

    val tableName = "user"

    // 配置相关信息

    val conf = HBaseUtils.getHBaseConfiguration(tableName)


    val indataRDD = sc.makeRDD(Array("100210,20","100310,30"))

    saveRddToHbase(tableName,indataRDD)
    sc.stop()
  }

  def saveRddToHbase(tableName:String,indataRDD:RDD[String]): Unit ={
   //indataRDD_getNumPartitions:2
   //indataRDD_length:3
   //rdd:1002,10 rdd:1003,10 rdd:1004,50 x:non-empty iterator
   //x:non-empty iterator
    val result = indataRDD.collect()
    println("indataRDD_getNumPartitions:"+indataRDD.getNumPartitions)
    println("indataRDD_length:"+result.length)

    println()
    indataRDD.foreachPartition(x=> {
      val conf = HBaseUtils.getHBaseConfiguration(tableName)
      conf.set(TableOutputFormat.OUTPUT_TABLE,tableName)
      x.foreach(y => {
        println("y:"+y)
        val arr = y.split(",")
        val key = arr(0)
        val value = arr(1)
      println("key:"+key)
      println("value:"+value)
      //Return statements aren't allowed in Spark closures
      if(!"0".equals(value)){
      val htable = HBaseUtils.getTable(conf,tableName)
      val put = new Put(Bytes.toBytes(key))
      put.add(Bytes.toBytes("info"),Bytes.toBytes("age"),Bytes.toBytes(value))
      println("begin put")
      htable.put(put)
      println("end put")
      }
//      x.foreach(y => {
//        println("y:"+y)
//        val arr = y.split(",")
//
      })
    })


  }
}
