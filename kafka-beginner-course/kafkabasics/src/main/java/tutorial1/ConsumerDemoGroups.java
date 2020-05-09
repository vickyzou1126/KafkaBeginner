package tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

//test by creating multiple instances , mimic multiple consumers and each consumer points to specific partitions
public class ConsumerDemoGroups {
    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(ConsumerDemoGroups.class.getName());
        String bootstrapServers = "127.0.0.1:9092";
        String groupId = "my-fifth-app";
        String topic= "first_topic";

        //create consumer configs
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");

        //create consumer
        KafkaConsumer<String,String> consumer = new KafkaConsumer<String, String>(properties);

        //subscribe consumer to our topic
        //consumer.subscribe(Arrays.asList("first_topic","second_topic"));
        consumer.subscribe(Collections.singleton(topic));

        //poll for new data
        while (true){
            ConsumerRecords<String,String> records =consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String,String> record : records){
                logger.info("Key: "+record.key() +", Value: "+record.value());
                logger.info("partition: "+record.partition() +", Offset: "+record.offset());
            }
        }
    }
}
