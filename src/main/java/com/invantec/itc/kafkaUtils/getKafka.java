/**
* <p>Title: getKafka.java</p>  
* <p>Description: </p>   
* @author Hu.Yue
* @date Dec 28, 2020  
* @version 1.0 
*/  


package com.invantec.itc.kafkaUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonMappingException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

/**
 * @author Hu.Yue
 *
 */
public class getKafka {
	
	private Properties properties = new Properties();
	private KafkaConsumer<String, String> consumer;
	private String topic = "student";
	Log logger = LogFactory.getLog(getKafka.class);
	
	public getKafka() {
		
		properties.setProperty("bootstrap.servers", "node01:9092");
		properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		properties.setProperty("group.id", "java_group1");
		properties.setProperty("auto.offset.reset", "earliest");	
		properties.setProperty("kafka.consumer.auto.offset.reset", "enableAutoCommit");
		consumer = new KafkaConsumer<String, String>(properties);
	}

	
	public void subscribeTopic() throws JsonMappingException, JsonProcessingException {
		
		List<String> topList = new ArrayList<String>();
		
		topList.add(topic);
		consumer.subscribe(topList);
		while(true) {
			ConsumerRecords<String, String> records = consumer.poll(100);
			for(ConsumerRecord<String, String> record : records) {
				System.out.println(record.key());
				System.out.println(record.value());
			}
		}
	}
	
	public static void main(String[] args) throws JsonMappingException, JsonProcessingException {
		getKafka kafkaConsumer = new getKafka();
		kafkaConsumer.subscribeTopic();
	}
}
