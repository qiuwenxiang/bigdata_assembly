/*
package com.kylin.assembly.strom.process;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.JSONException;
import org.json.simple.JSONObject;

import com.tydic.frame.cglib.CglibProxy;
import com.tydic.frame.kafka.Producer;
import com.tydic.ruleEngine.DateHelper;
import com.tydic.ruleEngine.po.DropBean;
import com.tydic.ruleEngine.po.MapEntry;
import com.tydic.ruleEngine.storm.RuleBase;
import com.tydic.ruleEngine.util.HbaseHelper;
import com.tydic.ruleEngine.util.RedisHelper;
import com.tydic.ruleEngine.util.RuleEngineAction;
import com.tydic.sqlparser.sql.visitor.SQLEvalVisitorUtils;


*/
/**
 * @Description	TODO
 * @ClassName	ScProcessImpl
 * @Date		2016年11月22日 下午4:52:01
 * @Author		qiuwenxiang@tydic.com
 * Copyright (c) All Rights Reserved, 2016.
 *//*


public class SnProcessImpl extends RuleEngineAction{

	private RedisHelper redisHelper=new RedisHelper();
	private static final Log LOG = LogFactory.getLog(SnProcessImpl.class);
	private static final String CONNECT="-";
	*/
/* (non-Javadoc)
	 * @see com.tydic.ruleEngine.util.RuleEngineAction#createWorkOrder()
	 *//*

	@Override
	public Boolean createWorkOrder(Map<String, String> processMap,
			Map<String, String> kafkaMap) throws Exception {

		 List<Map<String, String>> ruleList = redisHelper.getBaseConfigSort("RTA_CORE_RULE_EXPR","ORDER_ID",RedisHelper.ASC,processMap.get("RULE_FL_ID"));
		 final String phone=kafkaMap.get("phone");
		 for (Map<String, String> map : ruleList) {
			 String expr_before=map.get("EXECUTE_EXPR");
			 String expr_after=replaceVariables(expr_before,kafkaMap.get("phone"),kafkaMap);
			 LOG.info(expr_before+"转变为："+expr_after);
			 Object evalExpr = SQLEvalVisitorUtils.evalExpr(expr_after);
			 LOG.info("解析："+expr_after+"  返回值为："+evalExpr);
			 String[] data = evalExpr.toString().split(";");
			 for (int i = 0; i < data.length; i++) {
				 String[] data1=data[i].split(":");
				 kafkaMap.put(data1[0], data1[1]);
			}
			 String channels_group=kafkaMap.get("channels");
			
			 String obj_id_group=kafkaMap.get("sales");
			 String obj_id="";
			 if ("0".equals(channels_group)||"0".equals(obj_id_group)) {
				//找不到渠道or销售品分组
				 DropBean bean = new DropBean();
					bean.setAcc_nbr(phone);
					bean.setCampaign_id(kafkaMap.get("campaign_id"));
					bean.setFilt_expr_after(expr_after);
					bean.setFilt_expr_before(expr_before);
					bean.setFilt_reason(String.format("号码%s未获取渠道分组%s或销售品分组%s,丢弃,,,", phone,channels_group,obj_id_group));
					bean.setFilt_step(map.get("RULE_FL_ID"));
					LOG.info("信息丢弃,evalExpr值为"+evalExpr);
			        //活动id +年月日时分秒+电话号码
			        String rowkey=bean.getCampaign_id()+""+bean.getFilt_step()+""+kafkaMap.get("time")+""+bean.getAcc_nbr()+""+RuleBase.getRowKeyAdd(kafkaMap);
			        HbaseHelper.insertDropTable(rowkey,bean);
		    	    return false;  
			 }
			 List<Map<String, String>> channelList = redisHelper.getBaseConfig("RTA_CAMPAIGN_GROUP_INFO",channels_group,"");
			 if (obj_id_group != null) {
				 List<Map<String, String>> objList = redisHelper.getBaseConfig("RTA_CAMPAIGN_GROUP_INFO",obj_id_group,"");
				 obj_id=objList.get(0).get("SALES");
			}
			 for (int i = 0; i < channelList.size(); i++) {
				 final String campaign_id=kafkaMap.get("campaign_id");
				 final String channel_id=channelList.get(i).get("CHANNEL_ID");
				 final String region= kafkaMap.get("region");
				 final String msgAfter=replaceVariables(channelList.get(i).get("MSG"),phone,kafkaMap);
				 final String kafka_topic = kafkaMap.get("send_kafka_topic");
				 String rowkey=campaign_id+""+kafkaMap.get("time")+""+channel_id+""+phone;
				 
				 Map<String, String> info = redisHelper.getBaseConfigOnly("RTA_CORE_DATA_RESOURCE_INFO", kafkaMap.get("topic_head"));

				 String rowkey_list = info.get("ROWKEY_LIST");
				 if(!"null".equals(rowkey_list)){
					 String[] ss = rowkey_list.split(",");
					 for (String s:ss) {
						 rowkey+=kafkaMap.get(s);
					}
				 }
				 MapEntry<String, Object> params = new MapEntry<String, Object>(){{
					 put("campaign_id", campaign_id);
					 put("channel_id", channel_id);
					 put("msg", msgAfter);
					 put("region_id", region);
					 put("kafka_topic", kafka_topic);
					 put("acc_nbr", phone);
					 put("create_time", DateHelper.getDate(DateHelper.format1));
				 }};
				 
				 params.put("rowKey", rowkey);
				 params.put("obj_id",obj_id);
				 String param_list = info.get("PARAM_LIST");
				 if(!"null".equals(param_list)){
					 String[] ss = param_list.split(",");
					 for (String s:ss) {
						 params.put(s,kafkaMap.get(s));
					}
				 }
				 
				 try {
					Producer.doProcess(kafkaMap.get("send_kafka_topic"),JSONObject.toJSONString(params));
					params.remove("rowKey");
					params.put("msg", replaceBlank(msgAfter));
					HbaseHelper.insertWorkOrder(rowkey,params);
				} catch (Exception e) {
					// TODO Auto-generated catch block
					LOG.error("发送kafka或入hbase错误："+e.getMessage());
				}
				
			 }
			
		  }
		 return true;
	}

	
	*/
/**
	 * @param kafkaMap 
     * @param  
     * @Description 统计变量标签,陕西的规则
	 * @param  参数
	 * @return void 返回类型 
	 * @throws Exception 
	 *//*


	public Boolean countLabel(Map<String, String> processMap, Map<String, String> kafkaMap) throws Exception {
		// TODO Auto-generated method stub
		 List<Map<String, String>> ruleList = redisHelper.getBaseConfigSort("RTA_CORE_RULE_EXPR","ORDER_ID",RedisHelper.ASC,processMap.get("RULE_FL_ID"));
		 Map<String, String> hash =new HashMap<String, String>();
		 String phone=kafkaMap.get("phone");
			for (Map<String, String> map : ruleList) {
				 String expr=map.get("EXECUTE_EXPR");
				 String evalExpr = SQLEvalVisitorUtils.evalExpr(expr)+"";
				 //不可能为空
				 String[] s = evalExpr.split(",");
				for(String labelId:s){
				 String tableName=redisHelper.getBaseConfigOnly("LABEL_TABLE_VIEW",labelId).get("TABLE_NAME");
				  if (tableName.equals("RTA_CORE_FEATURE_LABEL")) {
					  String pid = redisHelper.getBaseConfigOnly(tableName,labelId).get("BUSI_CHIP_NAME");
					  String label_code = redisHelper.getBaseConfigOnly(tableName,labelId).get("LABEL_CODE").toLowerCase();
					  String hbaseName=redisHelper.getBaseConfigOnly("RTA_CORE_LABEL_CATEGORY",pid).get("SOURCE_TABLE_ENNAME");
					  Map<String, Object> userInfo = CglibProxy.getInstance().getJavaBean(new HbaseHelper()).getDataByRowKey(hbaseName, phone);
					       if (userInfo == null || "".equals(userInfo.get(label_code))) {
					    	   DropBean bean = new DropBean();
								bean.setAcc_nbr(phone);
								bean.setCampaign_id(kafkaMap.get("campaign_id"));
								bean.setFilt_expr_after(expr);
								bean.setFilt_expr_before(evalExpr);
								bean.setFilt_reason(String.format("查询用户phone:%s,标签id:%s,所属表:%s,所属字段:%s失败,丢弃,,,", phone,labelId,hbaseName,label_code));
								bean.setFilt_step(map.get("RULE_FL_ID"));
								LOG.info("信息丢弃,evalExpr值为"+evalExpr);
						        //活动id +年月日时分秒+电话号码
						        String rowkey=bean.getCampaign_id()+""+bean.getFilt_step()+""+kafkaMap.get("time")+""+bean.getAcc_nbr();
						        HbaseHelper.insertDropTable(rowkey,bean);
					    	   return false;  
					       }else{
					    	   hash.put(label_code, userInfo.get(label_code)+"");
					       }
				      }
				}
			}
			 if (hash.size()!=0) {
				 redisHelper.getJedis().hmset(RedisHelper.USERLABEL+phone,hash);
			 }else{
				 LOG.info("该活动没有实时标签需要生成");
			 }
			return true;
	}
	
	public static void main(String[] args) throws JSONException {
		*/
/*Map<String, Object> params = new HashMap<String, Object>();
		params.put("topic_head", "ss-001");
		params.put("phone", "1878291022");
		Map<String, Object> data=new HashMap<String, Object>();
		data.put("state", "add");
		params.put("data", data);
		System.out.println(JSONObject.toJSONString(params));*//*

		String str="{'topic_head':'DPI_ITV_COUNT','phone':'18782915','data':{'ss-001':50,'ss-002':0,'month':201611},'time':20161128171452}";
	    org.json.JSONObject strJson = new  org.json.JSONObject(str);
		Map<String ,String> map = new HashMap<String, String>();
		try {
			if ("".equals(strJson.get("topic_head")) || "".equals(strJson.get("time")) || "".equals(strJson.get("phone"))) {
				System.out.println("出错");
			}
			
			System.out.println(map);
		} catch (JSONException e) {
			System.out.println(e.getMessage());
			// TODO Auto-generated catch block
		}
		System.out.println("end---");
		 
	}
	
	
}
*/
