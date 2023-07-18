package com.blackpearl.marketing.rule_manager.service;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.http.HttpHost;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.index.query.*;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.roaringbitmap.RoaringBitmap;

import java.io.IOException;

public class ProfileConditionQueryService {

    private RestHighLevelClient client;
    private SearchRequest request;

    public ProfileConditionQueryService() {
        client = new RestHighLevelClient(RestClient.builder(new HttpHost("node01", 9200, "http")));
        request = new SearchRequest("profiles_test");
    }


    // [
    //   {"tagId":"tg01","compareType":"eq","compareValue":"3"},
    //   {"tagId":"tg04","compareType":"match","compareValue":"运动"}
    // ]
    public RoaringBitmap queryProfileUsers(JSONArray profileConditions) throws IOException {

        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();

        for (int i = 0; i < profileConditions.size(); i++) {

            // {"tagId":"tg01","compareType":"eq","compareValue":"3"},
            JSONObject condition = profileConditions.getJSONObject(i);

            String tagId = condition.getString("tagId");
            String compareType = condition.getString("compareType");
            String compareValue = condition.getString("compareValue");

            // TODO 组合查询条件，目前都是用的 && 的关系，后期还要想办法使用 && 和 || 随机组合
            // 比较类型：大于，小于....
            switch (compareType) {
                case "lt":
                    RangeQueryBuilder lt = QueryBuilders.rangeQuery(tagId).lt(compareValue);
                    if (compareValue.matches("\\\\d+(.\\\\d+)?\"")) {
                        lt = QueryBuilders.rangeQuery(tagId).lt(Float.parseFloat(compareValue));
                    }
                    boolQueryBuilder.must(lt);
                    break;
                case "gt":
                    RangeQueryBuilder gt = QueryBuilders.rangeQuery(tagId).gt(compareValue);
                    if(compareValue.matches("\\d+(.\\d+)?")){
                        gt = QueryBuilders.rangeQuery(tagId).gt(Float.parseFloat(compareValue));
                    }
                    boolQueryBuilder.must(gt);
                case "ge":
                    RangeQueryBuilder gte = QueryBuilders.rangeQuery(tagId).gte(compareValue);
                    if(compareValue.matches("\\d+(.\\d+)?")){
                        gte = QueryBuilders.rangeQuery(tagId).gte(Float.parseFloat(compareValue));
                    }
                    boolQueryBuilder.must(gte);
                    break;
                case "le":
                    RangeQueryBuilder lte = QueryBuilders.rangeQuery(tagId).lte(compareValue);
                    if(compareValue.matches("\\d+(.\\d+)?")){
                        lte = QueryBuilders.rangeQuery(tagId).lte(Float.parseFloat(compareValue));
                    }
                    boolQueryBuilder.must(lte);
                    break;
                case "between":
                    String[] fromTo = compareValue.split(",");
                    RangeQueryBuilder btw = QueryBuilders.rangeQuery(tagId).from(fromTo[0],true).to(fromTo[1],true);
                    if(fromTo[0].matches("\\d+(.\\d+)?") && fromTo[1].matches("\\d+(.\\d+)?")){
                        btw = QueryBuilders.rangeQuery(tagId).from(Float.parseFloat(fromTo[0]),true).to(Float.parseFloat(fromTo[1]),true);
                    }
                    boolQueryBuilder.must(btw);
                    break;
                default:
                    MatchQueryBuilder match = QueryBuilders.matchQuery(tagId, compareValue);
                    boolQueryBuilder.must(match);
            }
        }

        // 将查询条件构成一个查询请求
        request.source(new SearchSourceBuilder().query(boolQueryBuilder));

        // 用ES客户端，发出查询请求
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        SearchHits hits = response.getHits(); // 命中人群

        // 遍历查询结果，并将每个符合规则条件用户的guid添加到bitmap中
        RoaringBitmap bitmap = RoaringBitmap.bitmapOf();
        for (SearchHit hit : hits) {
            int guid = (int) hit.getSourceAsMap().get("guid");
            bitmap.add(guid);
        }


        // 关闭ES客户端
        // client.close();



        return bitmap;
    }
}
