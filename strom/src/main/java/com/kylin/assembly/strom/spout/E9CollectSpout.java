package com.kylin.assembly.strom.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import java.util.Map;


public class E9CollectSpout extends BaseRichSpout {
	
	private String message="DPI_ITV_COUNT#~#1#~#18081123540#~#100#~#50#~#50#~#201605";

	private SpoutOutputCollector spoutOutputCollector;
  
	@Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    	 outputFieldsDeclarer.declare(new Fields("message"));
    }

    @Override
    public void open(Map map, TopologyContext topologyContext,
                     SpoutOutputCollector spoutOutputCollector) {
    	  this.spoutOutputCollector = spoutOutputCollector;
    }

    @Override
    public void nextTuple() {
    	try {
    			System.out.println("发送消息。。。");
				Thread.sleep(1000);
				this.spoutOutputCollector.emit(new Values(message),"message");
				//this.spoutOutputCollector.emit("message",new Values(message)); 错误方式
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }
    
    @Override
    public void ack(Object msgId) {
    	// TODO Auto-generated method stub
    	System.out.println("成功发送-----spout");
    	super.ack(msgId);
    }
    
    @Override
    public void fail(Object msgId) {
    	// TODO Auto-generated method stub
    	System.out.println("失败发送------spout");
    	super.fail(msgId);
    }
    
}
