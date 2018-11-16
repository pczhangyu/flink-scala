package com.travelsky.tap.sink;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;


/**
 * Created by pczhangyu on 2018/11/13.
 */
public class ElasticsearchSinker extends RichSinkFunction implements ElasticsearchSinkFunction{

    @Override
    public void process(Object element, RuntimeContext ctx, RequestIndexer indexer) {
        String elementJson = JSONObject.toJSONString(element);
        this.createIndexRequest(JSONObject.parseObject(elementJson));
    }

    private IndexRequest createIndexRequest(JSONObject jsonObject) {
        return Requests.indexRequest()
                .index("flink-sql-result")
                .type("stream")
                .source(jsonObject);
    }



}
