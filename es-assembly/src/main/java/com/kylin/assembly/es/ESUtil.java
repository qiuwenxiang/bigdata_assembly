package com.kylin.assembly.es;

import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.search.sort.SortBuilders;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

public class ESUtil {

	private static final int MAX_AMOUNT=5000;
	/**
	 * 构建ES索引
	 * @param map
	 * @param index
	 * @param type
	 */
	public void buliderEsIndex(Map<String, String> map, String index, String type)
	{

		if (checkIndexExists(index))
		{
			deleteEsIndex(index);
		}


		buliderIndex(index);

		XContentBuilder esMapping = buliderEsMapping(map);

		//构建type
		PutMappingRequest putMappingRequest = Requests.putMappingRequest(index).type(type).source(esMapping);
		EsClient.getEsClient().admin().indices().putMapping(putMappingRequest).actionGet();

		//关闭ES client
		EsClient.getEsClient().close();
	}

	/**
	 * 检查ES中索引是否存在
	 *
	 * @param index
	 * @return
	 */
	public static boolean checkIndexExists(String index)
	{
		boolean indexExists = EsClient.getEsClient().admin().indices().prepareExists(index).execute().actionGet().isExists();
		return indexExists;
	}

	/**
	 * 检查index索引中type是否存在
	 *
	 * @return
	 */
	public static boolean checkTypeExists(String index, String type)
	{
		boolean typeExists = EsClient.getEsClient().admin().indices().prepareTypesExists(index).setTypes(type).execute().actionGet().isExists();
		return typeExists;
	}

	/**
	 * 构建索引
	 *
	 * @param index
	 */
	private void buliderIndex(String index)
	{
		EsClient.getEsClient().admin().indices().create(new CreateIndexRequest(index));
	}

	/**
	 * 构建索引列，和数据类型
	 *
	 * @param map
	 * @return
	 */
	private XContentBuilder buliderEsMapping(Map<String, String> map)
	{
		XContentBuilder mapBuilder = null;
		try
		{
			mapBuilder = jsonBuilder();
			mapBuilder.startObject().startObject("properties");

			if (map != null && map.size() > 0)
			{
				for (Map.Entry<String, String> entry : map.entrySet())
				{
					//date类型数据，在es中构建索引时，要设置format和format格式
					if ("date".equals(entry.getValue()))
					{
						mapBuilder.startObject(entry.getKey()).field("type", entry.getValue()).field("format", "yyyy-MM-dd HH:mm:ss").endObject();
					} else
					{
						mapBuilder.startObject(entry.getKey()).field("type", entry.getValue()).field("store", true).endObject();
					}

				}
			}
			mapBuilder.endObject().endObject();
			System.out.println(mapBuilder.string());
		} catch (IOException e)
		{
			e.printStackTrace();
		}
		return mapBuilder;
	}

	/**
	 * 删除索引
	 *
	 * @param index
	 * @return false 表明索引不存在或删除失败
	 */
	public static boolean deleteEsIndex(String index){
		if (checkIndexExists(index)){
			DeleteIndexResponse rep = EsClient.getEsClient().admin().indices().prepareDelete(index).execute().actionGet();
			return rep.isAcknowledged();
		}
		return false;
	}

	private static Settings settings = Settings.builder().put("cluster.name", "mycluster")
			.put("transport.type", "netty3").put("http.type", "netty3").build();



	public static SearchHit[] get(TransportClient client, String esIndex, String esType, String spider, String url) {
		SearchResponse response = client.prepareSearch(esIndex).setTypes(esType).setVersion(true)
				// .setScroll(new TimeValue(60000))
				// .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
				// .setQuery(QueryBuilders.matchPhraseQuery("spider", spider))
				// // Query
				.setQuery(QueryBuilders.termQuery("url", url)) // QueryBuilder
				.execute().actionGet();

		return response.getHits().getHits();
	}

	public static boolean checkIdExists(TransportClient client, String esIndex, String esType, String _id) {
		return client.prepareGet(esIndex, esType, _id).execute().actionGet().isExists();
	}

	public static GetResponse getById(TransportClient client, String esIndex, String esType, String _id) {
		return client.prepareGet(esIndex, esType, _id).setOperationThreaded(false).get();
	}

	public static boolean index(TransportClient client, String esIndex, String esType, byte[] json, String _id) {
		System.out.println("es:" + new String(json));
		try {
			IndexResponse response = null;
			// 建立索引
			if (_id != null){
				response = client.prepareIndex(esIndex, esType, _id).setSource(json).get();
			}
			else{
				response = client.prepareIndex(esIndex, esType).setSource(json).get();
			}
			return response.getVersion() > 0;

//			return response.isCreated();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
		}
		return false;
	}

	public static void deleteById(TransportClient client, String esIndex, String esType, String id) {
		client.prepareDelete(esIndex, esType, id).execute().actionGet();
	}

	public static void close(TransportClient client) {
		client.close();
	}

	public static boolean exists(TransportClient client, String esIndex) {
		return client.admin().indices().prepareExists(esIndex).execute().actionGet().isExists();
	}

	public static List<SearchHit[]> querySearch(String index, String type,String term,String queryString)  {

		Client client = EsClient.getEsClient();
		SearchResponse response = client.prepareSearch(index)
				.setTypes(type)
				// 设置查询类型
				// 1.SearchType.DFS_QUERY_THEN_FETCH = 精确查询
				// 2.SearchType.SCAN = 扫描查询,无序
				// 3.SearchType.COUNT = 不设置的话,这个为默认值,还有的自己去试试吧
				.addSort(SortBuilders.fieldSort("_doc"))
				.setSearchType(SearchType.QUERY_THEN_FETCH)
				// 设置查询关键词
				/*
				.setQuery(QueryBuilders.matchQuery(term, queryString))
				*/
				// 设置查询数据的位置,分页用
				// 设置查询结果集的最大条数
				// 设置是否按查询匹配度排序
                .setSize(MAX_AMOUNT)
				.setScroll(TimeValue.timeValueMinutes(8))
				.execute()
				.actionGet();
		SearchHit[] hits = response.getHits().getHits();
        long total = response.getHits().getTotalHits();
        List<SearchHit[]> list = new ArrayList<>();
		list.add(hits);
		int i=1;
		if ( total > MAX_AMOUNT ){
			do {
				// 最后就是返回搜索响应信息
                response =  client.prepareSearchScroll(response.getScrollId())
                        .setScroll(TimeValue.timeValueMinutes(8))
						.execute().actionGet();
				i++;
				list.add(response.getHits().getHits());
				if (response.getHits().getHits().length < MAX_AMOUNT ){
					break;
				}
			} while (true);
		}

		System.out.println("-----------------在["+term+"]中搜索关键字["+queryString+"]---------------------");
		System.out.println("共匹配到:"+ response.getHits().getTotalHits()+"条记录!,通过"+i+"次查询");
		return list;
	}
	public static Map<String, Object> search(String key,String index,String type,int start,int row){
		SearchRequestBuilder builder = EsClient.getEsClient().prepareSearch(index);
		builder.setTypes(type);
		builder.setFrom(start);
		builder.setSize(row);
		//设置高亮字段名称
		/*builder.addHighlightedField("title");
		builder.addHighlightedField("describe");
		//设置高亮前缀
		builder.setHighlighterPreTags("<font color='red' >");
		//设置高亮后缀
		builder.setHighlighterPostTags("</font>");*/
		builder.setSearchType(SearchType.DFS_QUERY_THEN_FETCH);
		if(StringUtils.isNotBlank(key)){
//          builder.setQuery(QueryBuilders.termQuery("title",key));
			builder.setQuery(QueryBuilders.multiMatchQuery(key, "title","describe"));
		}
		builder.setExplain(true);
		SearchResponse searchResponse = builder.get();

		SearchHits hits = searchResponse.getHits();
		long total = hits.getTotalHits();
		Map<String, Object> map = new HashMap<String,Object>();
		SearchHit[] hits2 = hits.getHits();
		map.put("count", total);
		List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();
		for (SearchHit searchHit : hits2) {
			Map<String, HighlightField> highlightFields = searchHit.getHighlightFields();
			HighlightField highlightField = highlightFields.get("title");
			Map<String, Object> source = searchHit.getSource();
			if(highlightField!=null){
				Text[] fragments = highlightField.fragments();
				String name = "";
				for (Text text : fragments) {
					name+=text;
				}
				source.put("title", name);
			}
			HighlightField highlightField2 = highlightFields.get("describe");
			if(highlightField2!=null){
				Text[] fragments = highlightField2.fragments();
				String describe = "";
				for (Text text : fragments) {
					describe+=text;
				}
				source.put("describe", describe);
			}
			list.add(source);
		}
		map.put("dataList", list);
		return map;
	}
}
