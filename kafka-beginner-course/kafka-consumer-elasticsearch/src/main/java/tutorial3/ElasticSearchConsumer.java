package tutorial3;

import com.google.gson.JsonParser;
import org.apache.http.HttpHost;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ElasticSearchConsumer {
    public static RestHighLevelClient createClient(){
        String hostname="localhost";
        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        RestClientBuilder builder = RestClient.builder(
                new HttpHost(hostname,9200,"http"))
                .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback(){
                    @Override
                    public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder){
                        return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                    }
                }
        );
        RestHighLevelClient client = new RestHighLevelClient(builder);
        return client;
    }
    public static KafkaConsumer<String,String> createConsumer(String topic){
        String bootstrapServers = "127.0.0.1:9092";
        String groupId = "kafka-demo-elasticsearch";

        //create consumer configs
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        //commit offset
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,"false");
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,"100");
        //create consumer
        KafkaConsumer<String,String> consumer = new KafkaConsumer<String, String>(properties);
        consumer.subscribe(Arrays.asList(topic));

        return consumer;
    }
    public static void main(String[] args) throws IOException {
        Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer.class.getName());
        RestHighLevelClient client= createClient();

        KafkaConsumer<String,String> consumer = createConsumer("twitter_tweets");
        //poll for new data
        while (true){
            ConsumerRecords<String,String> records =consumer.poll(Duration.ofMillis(100));

            Integer recordCount = records.count();
            logger.info("Received "+recordCount +" records");

            BulkRequest bulkRequest = new BulkRequest();

            for (ConsumerRecord<String,String> record : records){
                //two  strategies
                //kafka generic ID
                //String id =record.topic()+"_"+record.partition()+"_"+record.offset();
                //twitter feed specific id
                try{
                    String id = extractIdfromTweet(record.value());
                    //where we insert dat into elasticsearch
                    IndexRequest indexRequest = new IndexRequest(
                            "twitter",
                            "tweets",
                            id//this is to make out consumer idemponent
                    ).source(record.value(), XContentType.JSON);

                    bulkRequest.add(indexRequest); //we add to our bulk request , rather than iterate records one by one
                /*IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
                logger.info(indexResponse.getId());
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }*/
                }catch (NullPointerException e){
                    logger.warn("skipping bad data: "+record.value());
                }
            }
            if(recordCount > 0){
                BulkResponse bulkItemResponses = client.bulk(bulkRequest,RequestOptions.DEFAULT);
                logger.info("Committing offsets...");
                consumer.commitAsync();
                logger.info("Offsets haven been committed");
                try{
                    Thread.sleep(1000);
                }catch (InterruptedException e){
                    e.printStackTrace();
                }
            }
        }
        //client.close();
    }
    private  static JsonParser jsonParser = new JsonParser();

    private static String extractIdfromTweet(String tweetJson){
        return jsonParser.parse(tweetJson).getAsJsonObject().get("id_str")
                .getAsString();
    }
}
