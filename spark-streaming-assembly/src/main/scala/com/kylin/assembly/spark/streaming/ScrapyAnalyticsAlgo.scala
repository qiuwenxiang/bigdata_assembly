package com.kylin.assembly.spark.streaming


import com.alibaba.fastjson.{JSON, JSONObject}
import com.kylin.assembly.common.GlobalParamValue
import com.kylin.assembly.spark.streaming.constant.StreamConstant
import kafka.api.{OffsetRequest, PartitionOffsetRequestInfo, TopicMetadataRequest}
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Duration, StreamingContext}
import kafka.common.TopicAndPartition
import kafka.consumer.SimpleConsumer
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import kafka.utils.{ZKGroupTopicDirs, ZKStringSerializer, ZkUtils}
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
      val appName = GlobalParamValue.get(StreamConstant.APP_NAME)

      if (true){
        conf = new SparkConf().setAppName(appName).setMaster("local[1]")
      }else{
        conf = new SparkConf().setAppName(appName)
      }

      // Create a StreamingContext with the given master URL
      val ssc = new StreamingContext(conf, new Duration(5 * 1000))

      // Kafka configurations
      var topic  = GlobalParamValue.get(StreamConstant.KAFKA_TOPICS)
      val topics = Set(topic)
      val topicDirs = new ZKGroupTopicDirs(GlobalParamValue.get(StreamConstant.KAFKA_GROUP), topic)


      val zkTopicPath = s"${topicDirs.consumerOffsetDir}"
      val brokers = GlobalParamValue.get(StreamConstant.METADATA_BROKER_LIST)
      val kafkaParam = Map[String, String](
        "metadata.broker.list" -> brokers,
        "serializer.class" -> "kafka.serializer.StringEncoder",
         "auto.offset.reset" ->auto_offset_reset
      )

      // Create a direct stream
      val zkClient = new ZkClient(GlobalParamValue.get(StreamConstant.ZOOKEEPER_CONNECT))
      zkClient.setZkSerializer(ZKStringSerializer)
      //查询该路径下是否字节点（组消费模式路径为/consumers/%s/offsets/%s/%s）
      val children = zkClient.countChildren(s"${topicDirs.consumerOffsetDir}") //查询该路径下是否字节点（默认有字节点为我们自己保存不同 partition 时生成的）

      var kafkaStream : InputDStream[(String,String)] = null
      var fromOffsets : Map[TopicAndPartition,Long] = Map()
      if (children >0){
        val getLeaderConsumer = new SimpleConsumer("liebao235.test.com", 6667, 10000, 10000, "OffsetLookup") // 第一个参数是 kafka broker 的host，第二个是 port
        val req = new TopicMetadataRequest(List(topic), 0)
        val res = getLeaderConsumer.send(req)
        val topicMetaOption = res.topicsMetadata.headOption
        // 将partitions 转为  partitionId->leader 映射，方便下一步获取有效offset
        val partitions = topicMetaOption match {
          case Some(tm) =>
            tm.partitionsMetadata.map(pm => (pm.partitionId, pm.leader.get.host)).toMap[Int, String]  // 将结果转化为 partition -> leader 的映射关系
          case None =>
            Map[Int, String]()
        }
        for (i <- 0 until children){
          val partitionOffset = zkClient.readData[String](s"${topicDirs.consumerOffsetDir}/${i}")
          val tp = TopicAndPartition(topic,i)
          val requestMin = OffsetRequest(Map(tp -> PartitionOffsetRequestInfo(OffsetRequest.EarliestTime, 1)))

          val consumerMin = new SimpleConsumer(partitions(i), 6667, 10000, 10000, "getMinOffset")  //注意这里的 broker_host，因为这里会导致查询不到，解决方法在下面
          val curOffsets = consumerMin.getOffsetsBefore(requestMin).partitionErrorAndOffsets(tp).offsets
          var nextOffset = partitionOffset.toLong
          LOGGER.info("topic[" + topic + "] partition[" + i + "] curOffsets.head[" + curOffsets.head + "] zk中offsets "+ partitionOffset)
          if (curOffsets.length > 0 && nextOffset < curOffsets.head) {  // 通过比较从 kafka 上该 partition 的最小 offset 和 zk 上保存的 offset，进行选择
            nextOffset = curOffsets.head
          }
          fromOffsets += (tp -> nextOffset)
          /*fromOffsets += (tp -> partitionOffset.toLong)*/
          LOGGER.info("最终消费 topic[" + topic + "] partition[" + i + "] offset[" + partitionOffset + "] @@@@@@")
        }
        val messageHandler = (mmd : MessageAndMetadata[String, String]) => (mmd.topic, mmd.message())
        kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](ssc, kafkaParam, fromOffsets, messageHandler)
      }else{
        kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParam, topics)
      }

      var offsetRanges = Array[OffsetRange]()
      var events = kafkaStream.transform{ rdd =>
         offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        for (o <- offsetRanges) {
          LOGGER.info(s"topic  ${o.topic}  partition ${o.partition}  fromoffset ${o.fromOffset} 打算消费到  untiloffset ${o.untilOffset}")
        }
          rdd
      }
        .persist(StorageLevel.MEMORY_AND_DISK)
        .map(line =>{
          /*try {
            val data = JSON.parseObject(line._2)
            Some(data)
          } catch {
            case  ex:Exception=>{
              LOGGER.error("ERROR to json obj error.\nsource: " + line)
              ex.printStackTrace()
              None
            }
          }*/
          line
        }).filter(obj => obj != None)
        .foreachRDD(rdd =>{

          rdd.foreachPartition( partitionOfRecord =>{
            partitionOfRecord.foreach(pair => {
              LOGGER.info("qiuwx"+pair)
            })
          })

          // rdd 完成后，更新offsetRanges
          for (o <- offsetRanges) {
            val zkPath = s"${topicDirs.consumerOffsetDir}/${o.partition}"
            ZkUtils.updatePersistentPath(zkClient, zkPath, o.untilOffset.toString)  //将该 partition 的 offset 保存到 zookeeper
            LOGGER.info(s"save topic  ${o.topic}  partition ${o.partition}  untiloffset ${o.untilOffset} ")
          }

        }
    )


      ssc.start()
      ssc.awaitTermination()

    }
  }
