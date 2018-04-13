package com.kylin.assembly.spark.streaming


import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Duration, StreamingContext}
import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import kafka.utils.ZKGroupTopicDirs
import org.I0Itec.zkclient.ZkClient
import org.slf4j.LoggerFactory


/**
  * streaming 入口带算法分析
  * Created by lenovo on 2018/1/16.
  */
object ScrapyAnalyticsAlgo {

  var LOGGER =LoggerFactory.getLogger(this.getClass.getName.stripSuffix("$"))
    def main(args: Array[String]): Unit = {
      var auto_offset_reset :String="largest"
      if (args.length > 0) {
        auto_offset_reset = args(0)
      }
      var conf : SparkConf = null

      // windows调试标志
      if (true){
        conf = new SparkConf().setAppName("ScrapyAnalyticsLocal").setMaster("local[1]")
      }else{
        conf = new SparkConf().setAppName("ScrapyAnalyticsLocal")
      }

      // Create a StreamingContext with the given master URL
      val ssc = new StreamingContext(conf, new Duration(5 * 1000))

      // Kafka configurations
      var topic  = s"test"
      val topics = Set(topic)
      val topicDirs = new ZKGroupTopicDirs("test_spark_streaming_group", topic)


      val zkTopicPath = s"${topicDirs.consumerOffsetDir}"
      val brokers = "liebao49.test.com:6667,liebao235.test.com:6667,liebao36.test.com:6667"
      val kafkaParam = Map[String, String](
        "metadata.broker.list" -> brokers,
        "serializer.class" -> "kafka.serializer.StringEncoder",
         "auto.offset.reset" ->auto_offset_reset
      )

      // Create a direct stream
      val zkClient = new ZkClient("liebao49.test.com:2181,liebao235.test.com:2181,liebao36.test.com:2181")
      val children = zkClient.countChildren(s"${topicDirs.consumerOffsetDir}") //查询该路径下是否字节点（默认有字节点为我们自己保存不同 partition 时生成的）

      var kafkaStream : InputDStream[(String,String)] = null
      var fromOffsets : Map[TopicAndPartition,Long] = Map()
      if (children >0){
        for (i <- 0 to children){
          val partitionOffset = zkClient.readData[String](s"${topicDirs.consumerOffsetDir}/${i}")
          val tp = TopicAndPartition(topic,i)
          fromOffsets += (tp -> partitionOffset.toLong)
          LOGGER.info("@@@@@@ topic[" + topic + "] partition[" + i + "] offset[" + partitionOffset + "] @@@@@@")
        }
        val messageHandler = (mmd : MessageAndMetadata[String, String]) => (mmd.topic, mmd.message())
        kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](ssc, kafkaParam, fromOffsets, messageHandler)
      }else{
        kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParam, topics)
      }


      var events = kafkaStream.persist(StorageLevel.MEMORY_AND_DISK)
        .map(line =>{
          try {
            val data = JSON.parseObject(line._2)
            Some(data)
          } catch {
            case  ex:Exception=>{
              LOGGER.error("ERROR to json obj error.\nsource: " + line)
              ex.printStackTrace()
              None
            }
          }
        }).filter(obj => obj != None)


      ssc.start()
      ssc.awaitTermination()

    }
  }
