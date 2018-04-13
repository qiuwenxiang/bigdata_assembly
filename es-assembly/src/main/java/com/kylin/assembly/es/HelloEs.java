package com.kylin.assembly.es;

import com.kylin.assembly.common.GlobalParamValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @description: HELLO
 * @author: kylin.qiuwx@foxmail.com
 * @create: 2018-04-13
 **/
public class HelloEs {

    private static Logger logger = LoggerFactory.getLogger(HelloEs.class);


    public static void main(String[] args) {
        EsClient.getEsClient();
        GlobalParamValue.paramMap.entrySet().forEach(entry -> {
            logger.info("key:[{}]  and  value:[{}]" , entry.getKey(),entry.getValue());
        });
    }
}
