package com.kylin.assembly.es.spring.service;

/**
 * Created by husl4 on 2017/7/19.
 */
public interface SearchConfigService {

    String get(String index, String key, String defaultValue);

    void set(String index, String key, String value);

    void remove(String index, String key);
}
