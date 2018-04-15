package com.kylin.assembly.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * @description: 从资源文件中读取全局参数的值
 * @author: kylin.qiuwx@foxmail.com
 * @create: 2018-04-11
 **/
public class GlobalParamValue {

    private static final String fileName="config.properties";

    private static Logger LOGGER = LoggerFactory.getLogger(GlobalParamValue.class);

    public static Map<String,String> paramMap = null;

    static {
        Properties properties = new Properties();
        InputStreamReader ipstream = null;
        try {
            ipstream = new InputStreamReader(GlobalParamValue.class.getClassLoader().getResourceAsStream(fileName), "UTF-8");
            properties.load(ipstream);
            paramMap = new HashMap<String, String>((Map) properties);
        } catch (UnsupportedEncodingException e) {
            LOGGER.error(e.getMessage());
        } catch (IOException e) {
            LOGGER.error(e.getMessage());
        }
    }

    public static String get(String fileName){
        return paramMap.get(fileName);
    }
}
