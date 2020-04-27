package com.cognizant.kafka.producer;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

public class KafkaBinaryProducer {

    public String sendMessage(String broker, String topicName, String fileName) throws Exception {

        try {

            Properties props = new Properties();

            props.put("bootstrap.servers", broker + ":9092");
            props.put("acks", "all");
            props.put("retries", 0);
            props.put("batch.size", 16384);
            props.put("linger.ms", 1);
            props.put("buffer.memory", 33554432);
            props.put("key.serializer",
                    "org.apache.kafka.common.serialization.StringSerializer");
            props.put("value.serializer",
                    "org.apache.kafka.common.serialization.ByteArraySerializer");

            Producer<String, byte[]> producer = new KafkaProducer<String, byte[]>(props);
            RecordMetadata m = producer.send(new ProducerRecord<String, byte[]>(topicName, readFromFile(fileName))).get();

            producer.close();

            return "Topic : '" + m.topic() + "' Partition : " + m.partition();
        } catch (Exception e) {
            throw e;
        }
    }

    public static void main(String[] args) throws Exception{

        String topicName = "test1";
        Properties props = new Properties();


        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
//        props.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer",
                "org.apache.kafka.common.serialization.ByteArraySerializer");

//        Producer<String, String> producer = new KafkaProducer<String, String>(props);

        Producer<String, byte[]> producer1 = new org.apache.kafka.clients.producer.KafkaProducer<String, byte[]>(props);

//        producer.send(new ProducerRecord<String, String>(topicName, Integer.toString(i), Integer.toString(i)));

        RecordMetadata m = producer1.send(new ProducerRecord<String, byte[]>(topicName, readFromFile("c:/JsonMessage.json"))).get();

        producer1.close();

        System.out.println("Topic : '" + m.topic() + "' Partition : " + m.partition());
    }


    private static byte[] readFromFile(String filePath)
            throws IOException {

        try {

            RandomAccessFile file = new RandomAccessFile(filePath, "r");
            file.seek(0);
            byte[] bytes = new byte[(int)file.length()];
            file.read(bytes);
            file.close();
            return bytes;
        } catch (Exception e) {
            throw e;
        }
    }
}
