package com.blackpearl.marketing.export;

import com.alibaba.fastjson2.JSON;
import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.http.HttpHost;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.HashMap;

public class Write2ES {

    public static void main(String[] args) throws IOException {

        // 使用ES客户端写数据

        RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(new HttpHost("node01", 9200, "http")));

        String profile1 = JSON.toJSONString(new ProfileCondition(1, 5, 10, "高富帅", new String[]{"高端家具", "汽车保养", "小罐咖啡"}));
        String profile2 = JSON.toJSONString(new ProfileCondition(2, 4, 20, "白富美", new String[]{"兰蔻精华液", "特仑苏牛奶", "香奈儿", "高尔夫球场", "高尔夫运动服饰", "汽车内饰"}));
        String profile3 = JSON.toJSONString(new ProfileCondition(3, 3, 15, "全职妈妈", new String[]{"惠氏奶粉牛奶", "宝宝润肤露", "运动健身计划"}));
        String profile4 = JSON.toJSONString(new ProfileCondition(4, 3, 14, "职场人士", new String[]{"兰蔻小黑瓶", "宝宝润肤露", "家用汽车购置攻略"}));


        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.add(
                new IndexRequest("profiles_test").source(profile1, XContentType.JSON),
                new IndexRequest("profiles_test").source(profile2, XContentType.JSON),
                new IndexRequest("profiles_test").source(profile3, XContentType.JSON),
                new IndexRequest("profiles_test").source(profile4, XContentType.JSON)
        );

        BulkResponse response = client.bulk(bulkRequest, RequestOptions.DEFAULT);
        if (response.status().getStatus() == 200) {
            System.out.println("批量数据插入成功");
        }

        // 单条插入
        // IndexRequest request = new IndexRequest("doeusers", "doc", "1").source(profile1, XContentType.JSON);
        // IndexResponse response = client.index(request, RequestOptions.DEFAULT);

        client.close();

        // todo 利用Flink-connector-es写数据

    }


    @Data
    @AllArgsConstructor
    private static class ProfileCondition {
        private int guid;
        private int tag01;
        private int tag02;
        private String tag03;
        private String[] tag04;
    }


}
