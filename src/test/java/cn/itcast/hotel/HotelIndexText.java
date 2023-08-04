package cn.itcast.hotel;

import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.client.indices.GetIndexResponse;
import org.elasticsearch.xcontent.XContentType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static cn.itcast.hotel.constants.HotelContants.MAPPING_HOTEL;

public class HotelIndexText {

    private RestHighLevelClient client;

    @BeforeEach
    void beforeEach(){
        this.client=new RestHighLevelClient(RestClient.builder(
                HttpHost.create("localhost:9200")
        ));
    }
    @AfterEach
    void afterEach() throws IOException {
        this.client.close();
    }
    @Test
    void test() {
        System.out.println(client);
    }
    @Test
    void createMapping() throws IOException {
        // 创建一个Request对象
        CreateIndexRequest hotel_index = new CreateIndexRequest("hotel_index");
        //请求参数
        hotel_index.source(MAPPING_HOTEL, XContentType.JSON);
        //发起请求
        client.indices().create(hotel_index, RequestOptions.DEFAULT);
    }
    @Test
    void deleteMapping() throws IOException {
        // 1。 创建一个删除的request对象
        DeleteIndexRequest hotel_index = new DeleteIndexRequest("hotel_index");
        // 2。 发请求
        client.indices().delete(hotel_index, RequestOptions.DEFAULT);
    }
    @Test
    void getMapping() throws IOException {
        // 1。 创建一个查看的request对象
        GetIndexRequest hotel_index = new GetIndexRequest("hotel_index");
        // 2。 发请求
        boolean exists = client.indices().exists(hotel_index, RequestOptions.DEFAULT);
        System.out.println(exists);
    }
}
