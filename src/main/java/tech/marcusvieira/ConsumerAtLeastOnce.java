package tech.marcusvieira;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.function.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerAtLeastOnce {

    private static Logger logger = LoggerFactory.getLogger(ConsumerAtLeastOnce.class);

    public static void main(String[] args) {
        String bootstrapServer = "127.0.0.1:9092";
        String groupId = "consumers-at-least-once";
        String topic = "example-topic";

        //create consumer consumerProps
        Properties consumerProps = new Properties();

        consumerProps.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 30000);
        consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);

        //create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(consumerProps);

        //subscribe a topic
        consumer.subscribe(Collections.singletonList(topic));

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

            logger.info("Consumer created");
            records.forEach(new Consumer<ConsumerRecord<String, String>>() {
                @Override
                public void accept(ConsumerRecord<String, String> record) {
                    logger.info("Metadata \n" +
                        "Key: " + record.key() + "\n" +
                        "Value: " + record.value() + "\n" +
                        "Topic: " + record.topic() + "\n" +
                        "Offset: " + record.offset() + "\n" +
                        "Partition: " + record.partition() + "\n" +
                        "Timestamp: " + record.timestamp() + "\n"
                    );
                }
            });
            //commit after processing the records
            consumer.commitSync();
        }
    }
}

