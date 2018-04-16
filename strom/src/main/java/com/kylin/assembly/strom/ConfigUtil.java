package com.kylin.assembly.strom;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;

/**
 * 配置文件解析
 * @package com.tydic.storm.config.Config.java
 * @title 
 * @description 
 * @author jiangyingxu
 * @date 2016-2-23
 * @version V1.0
 */
public class ConfigUtil {
	public final static Map<String, Map<String, String>> paramMap = new HashMap<String, Map<String, String>>();
	
	static{
		initParams("/hadoop.properties", "hadoop");
		initParams("/argument.properties", "argument");
//		initParams("/hadoop.properties", "hadoop");
	}
	
	public static Map<String, String> hadoopParams = paramMap.get("hadoop");
	
	/**
	 * 静态初始化参数配置
	 * @description 
	 * @author jiangyingxu
	 * @create 2016-3-1
	 * @param storm
	 * @param paramMapKey
	 */
	public static Map<String, Map<String, String>> initParams(String filePath, String paramMapKey) {
		InputStream in = ConfigUtil.class.getResourceAsStream(filePath);
		Properties propConfig = new Properties();
		try {
			propConfig.load(in);
		} catch (IOException e) {
			e.printStackTrace();
		}
		Set<Entry<Object, Object>> entrySetConf = propConfig.entrySet();
		Map params = new HashMap();
		for (Entry<Object, Object> entry : entrySetConf) {
			params.put(entry.getKey().toString().trim(), entry.getValue().toString().trim());
		}
		paramMap.put(paramMapKey, params);
		return paramMap;
	}
	
	/**
	 * 获取对应的参数配置
	 * @description 
	 * @author jiangyingxu
	 * @create 2016-3-1
	 * @param storm
	 * @param paramMapKey
	 */
	private static Map<String, String> getParams(String paramMapKey) {
		return paramMap.get(paramMapKey);
	}
	

	/**
	 * 通过key获取hadoop配置参数
	 * @description 
	 * @author jiangyingxu
	 * @create 2016-2-29
	 * @param key
	 * @return
	 */
	public static String getParamHadoop(String key){
		return paramMap.get("hadoop").get(key);
	}
	
	/**
	 * 通过key获取hadoop配置参数
	 * @description 
	 * @author jiangyingxu
	 * @create 2016-2-29
	 * @param key
	 * @return
	 */
	public static String getParamArgument(String key){
		return paramMap.get("argument").get(key);
	}
	
	
	public static void main(String[] args) {
		
	}
	
	
}
