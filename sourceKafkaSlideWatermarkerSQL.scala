package com.test.flink

import java.util.Properties

import com.google.gson.{JsonElement, JsonObject, JsonParser}
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.table.api.{EnvironmentSettings, Table}
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.types.Row
import org.apache.kafka.common.serialization.StringDeserializer
//import org.apache.log4j.{Level, Logger}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.scala._

case class MaxwellLog(databaseName:String,tableName:String,sqlType:String,sqlData:String,sqlTs:Long)
object KafkaSource1 {

  def main(args: Array[String]): Unit = {
//    Logger.getLogger("org").setLevel(Level.ERROR)
//    Logger.getLogger("org").setLevel(Level.DEBUG)
    //1、初始化Flink流计算的环境
    val streamEnv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //定义采用EventTime作为时间语义
    streamEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    streamEnv.setParallelism(1)
    val settings: EnvironmentSettings = EnvironmentSettings.newInstance().useOldPlanner().inStreamingMode().build()
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(streamEnv,settings)

    //修改并行度
    streamEnv.setParallelism(1) //默认所有算子的并行度为1
    //2、导入隐式转换


    //连接Kafka，并且Kafka中的数据是普通字符串（String）
    val props = new Properties()
    props.setProperty("bootstrap.servers","hadoop01:9092,hadoop102:9092,hadoop103:9092")

    props.setProperty("group.id","fink")
    props.setProperty("key.deserializer",classOf[StringDeserializer].getName)
    props.setProperty("value.deserializer",classOf[StringDeserializer].getName)
    props.setProperty("auto.offset.reset","latest")

    val stream: DataStream[String] = streamEnv.addSource(new FlinkKafkaConsumer[String]("flink",new SimpleStringSchema(),props))

    /*
    strem流，是 json结构数据，解析json

    {"database":"scm","table":"CM_VERSION","type":"update","ts":1585479790,"xid":1588740,"commit":true,
    "position":"mysql-bin.000008:15447571","server_id":1,"thread_id":65,"schema_id":1,
    "data":{"VERSION":"5.16.1","GUID":"4baa6042-cda4-496c-8bbb-1b0d5b788744","LAST_UPDATE_INSTANT":1585393525453,"TS":null,"HOSTNAME":"hadoop01/10.206.0.32","LAST_ACTIVE_TIMESTAMP":1585479790562},"old":{"LAST_ACTIVE_TIMESTAMP":1585479775554}}

    {"database":"hive","table":"CDS","type":"insert","ts":1585479944,"xid":1590819,
    "xoffset":2,"position":"mysql-bin.000008:15453783","server_id":1,"thread_id":122,"schema_id":1,
    "data":{"CD_ID":266}}

    {"database":"hive","table":"PARTITION_KEYS","type":"delete","ts":1585480844,"xid":1603505,
    "xoffset":24,"position":"mysql-bin.000008:15511606","server_id":1,"thread_id":121,"schema_id":5,
    "data":{"TBL_ID":269,"PKEY_COMMENT":"partition-key-2","PKEY_NAME":"p2","PKEY_TYPE":"int","INTEGER_IDX":1}}

     */
    val maxwellStream: DataStream[MaxwellLog] = stream.map(
      line => {
        val json = new JsonParser()
        val obj: JsonObject = json.parse(line).asInstanceOf[JsonObject]
        val databaseName: String = obj.get("database").toString()
        val tableName: String = obj.get("table").toString()
        var sqlType: String = obj.get("type").toString()
        val sqlData: String = obj.get("data").toString()
        val sqlTs: Long = obj.get("ts").toString().toLong
        if ( sqlType == "insert"){
          sqlType = """INS"""
        }
//        if ( sqlType){
//          new MaxwellLog(databaseName, tableName, sqlType, sqlData, sqlTs)
//
//        }
        new MaxwellLog(databaseName, tableName, sqlType, sqlData, sqlTs)
      }
    )
      //引入Watermark，让窗口延迟 3秒 触发
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[MaxwellLog](Time.seconds(3)) {
      override def extractTimestamp(element: MaxwellLog) = {
        element.sqlTs
      }
    })

    tableEnv.registerDataStream("maxwell_log_tb",maxwellStream,'databaseName,'tableName,'sqlType,'sqlData,'sqlTs.rowtime)
//    tableEnv.sqlQuery("select hop_start(sqlTs,interval '5' second,interval '10' second)")
//    val result: Table = tableEnv.sqlQuery("select databaseName,tableName,sqlType,count(*)," +
//    val result: Table = tableEnv.sqlQuery("select databaseName,tableName,sqlType," +
    val result: Table = tableEnv.sqlQuery("select databaseName," +
      "hop_start(sqlTs,databaseName,tableName,sqlType,sqlData,interval '5' second,interval '10' second),hop_end(sqlTs,interval '5' second,interval '10' second) " +
      "from maxwell_log_tb  " +
//      "from maxwell_log_tb  where sqlType = 'INS' " +
//      "group by hop(sqlTs,interval '3' second ,interval '10' second),databaseName,tableName,sqlType")
      "group by hop(sqlTs,interval '5' second ,interval '10' second),databaseName")

    tableEnv.toRetractStream[Row](result)
      .print()

    tableEnv.execute("sql")
//    stream.print()
//
//
//    streamEnv.execute()


  }
}
