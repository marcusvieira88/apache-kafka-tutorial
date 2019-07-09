package tech.marcusvieira;

import java.util.Properties;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IdempotentProducer {

    private static Logger logger = LoggerFactory.getLogger(IdempotentProducer.class);

    public static void main(String[] args) {
        String bootstrapServer = "127.0.0.1:9092";

        //create producer properties
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        properties.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);
        properties.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);

        //create producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        for (int i = 0; i < 10; i++) {
            String key = "id_" + i;
            //create a record
            ProducerRecord<String, String> record = new ProducerRecord<String, String>("example-topic",
                key, "marcus test" + i);

            //send a message asynchronous
            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e == null) {
                        logger.info("Metadata \n" +
                            "Key: " + record.key() + "\n" +
                            "Value: " + record.value() + "\n" +
                            "Topic: " + recordMetadata.topic() + "\n" +
                            "Offset: " + recordMetadata.offset() + "\n" +
                            "Partition: " + recordMetadata.partition() + "\n" +
                            "Timestamp: " + recordMetadata.timestamp() + "\n"
                        );
                    } else {
                        logger.error("Erro when send a message", e);
                    }
                }
            });
        }

        //need to flush for send a message before close the program
        producer.flush();
        //close the producer
        producer.close();

    }
}
