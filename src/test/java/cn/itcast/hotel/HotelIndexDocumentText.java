package cn.itcast.hotel;

import cn.itcast.hotel.pojo.Hotel;
import cn.itcast.hotel.pojo.HotelDoc;
import cn.itcast.hotel.service.IHotelService;
import cn.itcast.hotel.service.impl.HotelService;
import com.alibaba.fastjson.JSON;
import org.apache.http.HttpHost;
import org.apache.lucene.search.TotalHits;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.suggest.Suggest;
import org.elasticsearch.search.suggest.SuggestBuilder;
import org.elasticsearch.search.suggest.SuggestBuilders;
import org.elasticsearch.search.suggest.completion.CompletionSuggestion;
import org.elasticsearch.xcontent.XContentType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.util.List;

import static cn.itcast.hotel.constants.HotelContants.MAPPING_HOTEL;
@SpringBootTest
public class HotelIndexDocumentText {
    @Autowired
    private IHotelService hotelService;
    private RestHighLevelClient client;

    @BeforeEach
    void beforeEach() {
        this.client = new RestHighLevelClient(RestClient.builder(
                HttpHost.create("localhost:9200")
        ));
    }

    @AfterEach
    void afterEach() throws IOException {
        this.client.close();
    }

    //测试自动补全功能
    @Test
    void testSuggest() throws IOException {
        //1。 准备request
        SearchRequest request = new SearchRequest("hotel");
        //dsl
        request.source().suggest(new SuggestBuilder().addSuggestion(
                "suggestions",
                SuggestBuilders.completionSuggestion("suggestion")
                        .prefix("h")
                        .skipDuplicates(true)
                        .size(10)
        ));
        //3. 发起请求
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        System.out.println(response);
        // 4.处理结
        Suggest suggest = response.getSuggest();
        //4.1.根据名称获取补全结
        CompletionSuggestion suggestion = suggest.getSuggestion("suggestions");
        // 4.2.获取options并遍历
        for (CompletionSuggestion.Entry.Option option : suggestion.getOptions()) {
            // 4.3.获取一个option中的text，也就是补全的词
            String text = option.getText().string();
            System.out.println(text);
        }
    }



    @Test
    void test() {
        System.out.println(client);
    }
    // 向ES里添加数据
    @Test
    void testAddDocument() throws IOException {
        // 0. 数据准备数据库先查询
        Hotel hotel = hotelService.getById(61083L);
        HotelDoc hotelDoc = new HotelDoc(hotel);
        //1. 准备request对象
        IndexRequest request = new IndexRequest("hotel").id(hotelDoc.getId().toString());
        //2. 准备json文档
        request.source(JSON.toJSONString(hotelDoc),XContentType.JSON);
        //3. 发送请求
        client.index(request,RequestOptions.DEFAULT);
    }
    @Test
    void testGetDocuments() throws Exception {
        //1. 准备request对象
        GetRequest hotel = new GetRequest("hotel", "61083");
        //2。 得到请求
        GetResponse documentFields = client.get(hotel, RequestOptions.DEFAULT);
        //3。 查询json反序列化
        String sourceAsString = documentFields.getSourceAsString();
        HotelDoc parse = JSON.parseObject(sourceAsString, HotelDoc.class);
        System.out.println(parse);
    }
    //修改文档
    @Test
    void testUpdateDocuments() throws IOException {
        //1. 准备request
        UpdateRequest request = new UpdateRequest("hotel", "61083");
        //2。准备请求参数
        request.doc(
            "price","952",
            "starName","四钻");
        // 3。 发送请求
        client.update(request,RequestOptions.DEFAULT);
    }
    //删除文档
    @Test
    void testDeleteDocuments() throws IOException {
        //1. 准备Request请求
        DeleteRequest request = new DeleteRequest("hotel", "61083");
        // 3。 发送请求
        client.delete(request,RequestOptions.DEFAULT);
    }
    //批量插入文档
    @Test
    void testBulkRequest() throws IOException {
        //0. Mp 查询数据库
        List<Hotel> hotels = hotelService.list();
        //1. 创建request
        BulkRequest bulkRequest = new BulkRequest();
        //2. 准备参数，添加多个新增的Request
        for(Hotel hotel:hotels){
            HotelDoc hotelDoc = new HotelDoc(hotel);
            //2.1 创建文档的对象
            bulkRequest.add(new IndexRequest("hotel")
                    .id(hotelDoc.getId().toString())
                    .source(JSON.toJSONString(hotelDoc),XContentType.JSON)
            );
        }
        //3. 发送请求
        client.bulk(bulkRequest,RequestOptions.DEFAULT);
    }

    @Test
    void testMatchall() throws IOException {
        //1. 准备request
        SearchRequest request = new SearchRequest("hotel");
        //2. 准备dsl
request.source().query(QueryBuilders.matchAllQuery());
        //3. 发送请求
        SearchResponse search = client.search(request, RequestOptions.DEFAULT);
        //System.out.println(search);


        handleResponse(search);

    }
    @Test
    void testMatch() throws IOException {
        //1. 准备request
        SearchRequest request = new SearchRequest("hotel");
        //2. 准备dsl
        request.source().query(QueryBuilders.matchQuery("all","如家"));
        //3. 发送请求
        SearchResponse search = client.search(request, RequestOptions.DEFAULT);
        //System.out.println(search);

        handleResponse(search);

    }

    @Test
    void testBoolean() throws IOException {
        //1. 准备request
        SearchRequest request = new SearchRequest("hotel");
        //2. 准备dsl
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        // 2.1 添加term
        boolQueryBuilder.must(QueryBuilders.termQuery("city", "上海"));
        boolQueryBuilder.filter(QueryBuilders.rangeQuery("price").lte(250));
        request.source().query(boolQueryBuilder);
        //3. 发送请求
        SearchResponse search = client.search(request, RequestOptions.DEFAULT);
        //System.out.println(search);

        handleResponse(search);

    }

    @Test
    void testAggregation() throws IOException {
        //1. 准备request
        SearchRequest request = new SearchRequest("hotel");
        //2. 准备dsl
        //2.1 设置size
        request.source().size(0);
        //2。2 聚合
        request.source().aggregation(AggregationBuilders
                .terms("brandagg")
                .field("brand")
                .size(10));
        //3. 发送请求
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        //4.解析结果
        Aggregations aggregations = response.getAggregations();
        //4.1 根据聚合名称获取聚合结果
        Terms brandTerms = aggregations.get("brandagg");
        //获取bucket
        List<? extends Terms.Bucket> buckets = brandTerms.getBuckets();
        //遍历
        for (Terms.Bucket bucket : buckets) {
            String keyAsString = bucket.getKeyAsString();
            System.out.println(keyAsString);
        }


    }


    private void handleResponse(SearchResponse search) {
        // 4. 响应解析
        SearchHits hits = search.getHits();
        // 4.1 总条数
        long value = hits.getTotalHits().value;
        System.out.println(value);
        //4。2 文档数组
        SearchHit[] hits1 = hits.getHits();
        // 4。3 遍历
        for (SearchHit hit : hits1){
            // 4.4 获取文档source
            String sourceAsString = hit.getSourceAsString();
            // fastjson反序列化
            HotelDoc hotelDoc = JSON.parseObject(sourceAsString, HotelDoc.class);
            System.out.println(hotelDoc);
        }
    }
}
