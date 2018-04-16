/*
package com.kylin.assembly.es.spring.service.impl;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.kylin.assembly.es.spring.service.SearchConfigService;
import org.elasticsearch.search.SearchHits;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.elasticsearch.core.ElasticsearchTemplate;
import org.springframework.data.elasticsearch.core.ResultsExtractor;
import org.springframework.data.elasticsearch.core.query.IndexQuery;
import org.springframework.data.elasticsearch.core.query.IndexQueryBuilder;
import org.springframework.data.elasticsearch.core.query.NativeSearchQueryBuilder;
import org.springframework.data.elasticsearch.core.query.SearchQuery;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

*/
/**
 * 搜索配置保存在搜索引擎
 *//*

@Service
public class SearchConfigServiceImpl implements SearchConfigService {

    @Autowired
    private ElasticsearchTemplate template;

    @Override
    public String get(String index, String key, String defaultValue) {
        List<String> ids = new ArrayList<String>();
        ids.add(key);

        SearchQuery searchQuery = new NativeSearchQueryBuilder().withIndices(index).withTypes(SearchConstants.ES_TYPE_CONFIG).withIds(ids).build();

        String value = template.query(searchQuery, (ResultsExtractor<String>) searchResponse -> {
            SearchHits searchHits = searchResponse.getHits();
            if(searchHits.getTotalHits() > 0) {
                return (String)searchHits.getAt(0).getSource().get("value");
            }

            return defaultValue;
        });

        return value;
    }

    @Override
    public void set(String index, String key, String value) {
        ObjectNode node = JacksonUtils.newObject();
        node.put("value",value);

        IndexQuery indexQuery = new IndexQueryBuilder().withIndexName(index).withType(SearchConstants.ES_TYPE_CONFIG).withId(key).withSource(node.toString()).build();
        template.index(indexQuery);
    }

    @Override
    public void remove(String index, String key) {
        template.delete(index, SearchConstants.ES_TYPE_CONFIG, key);
    }
}
*/
