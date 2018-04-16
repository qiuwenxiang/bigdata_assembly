package com.kylin.assembly.strom.topology;




import com.kylin.assembly.strom.Constant;
import com.kylin.assembly.strom.StormHandler;
import com.kylin.assembly.strom.blot.RuleEngineDealBlot;
import com.kylin.assembly.strom.blot.RuleEngineParseBlot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.kafka.KafkaSpout;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.AuthorizationException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;







/**
 * 
 * @Description	topology  main 函数
 * @ClassName	RuleEngineTopology
 * @Date		2016年5月19日 上午11:31:08
 * @Author		qiuwenxiang@tydic.com
 * Copyright (c) All Rights Reserved, 2016.
 */
public class RuleEngineTopology  {

    private static Logger LOG  = LoggerFactory.getLogger(RuleEngineTopology.class);
//    private boolean is_local = Constant.isDev;
    private boolean is_local = Constant.isDev;

    public void invokeBusiness() throws AlreadyAliveException, InvalidTopologyException, AuthorizationException {
        System.out.println("start..");
        TopologyBuilder builder = new TopologyBuilder();
        String receivceTopic1=Constant.receivceTopic1;
        String receivceTopic2=Constant.receivceTopic2;
        String receivceTopic3=Constant.receivceTopic3;
        LOG.info("topic1:" + receivceTopic1);
        LOG.info("topic2:" + receivceTopic2);
        LOG.info("topic3:" + receivceTopic3);
        
     
        builder.setSpout("reader1", new KafkaSpout(new StormHandler(receivceTopic1).spoutConf), Constant.SPOUT1_NUM);
        builder.setSpout("reader2", new KafkaSpout(new StormHandler(receivceTopic2).spoutConf), Constant.SPOUT2_NUM);
        builder.setSpout("reader3", new KafkaSpout(new StormHandler(receivceTopic3).spoutConf), Constant.SPOUT3_NUM);

       // builder.setBolt("parse_blot", new RuleEngineParseBlot(), Constant.PARSE_BLOT_NUM).shuffleGrouping("reader1");
        
        //解析3个topic的内容
        builder.setBolt("parse_blot", new RuleEngineParseBlot(), Constant.PARSE_BLOT_NUM).shuffleGrouping("reader1").shuffleGrouping("reader2").shuffleGrouping("reader3");
        builder.setBolt("deal_Blot", new RuleEngineDealBlot(), Constant.DEAL_BLOT_NUM).shuffleGrouping("parse_blot");

        Config conf = new Config();
        conf.setDebug(true);
        conf.setMessageTimeoutSecs(1000*60);
        LOG.info("is_local:" + Constant.isDev);
        LOG.info("TOPOLOGY_NAME:" + Constant.TOPOLOGY_NAME);
        if (!is_local) {
		//	conf.put(Config.NIMBUS_HOST, ConfigUtil.getParamStorm("nimbus.host"));
            System.out.println("publish..start");
            conf.setNumWorkers(Constant.WORK_NUM);
            StormSubmitter.submitTopologyWithProgressBar(Constant.TOPOLOGY_NAME, conf, builder.createTopology());
            System.out.println("publish ..end");
        } else {
           // conf.setMaxTaskParallelism(20);
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("rule_engine_topology", conf, builder.createTopology());
//            Utils.sleep(10000);
//            cluster.killTopology("topic11");
//            cluster.shutdown();

        }
    }
    public static void main(String[] args) {
        RuleEngineTopology handler = new RuleEngineTopology();
        if (args != null && args.length > 0) {
            handler.is_local = true;
        }
        try {
            handler.invokeBusiness();
        } catch (AlreadyAliveException e) {
            e.printStackTrace();
        } catch (InvalidTopologyException e) {
            e.printStackTrace();
        } catch (AuthorizationException e) {
            e.printStackTrace();
        }
    }
}
