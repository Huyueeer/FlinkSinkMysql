/**
* <p>Title: toKafka.java</p>  
* <p>Description: </p>   
* @author Hu.Yue
* @date Dec 28, 2020  
* @version 1.0 
*/  
package com.invantec.itc.kafkaUtils;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.Logger;

import com.alibaba.fastjson.JSON;
import com.invantec.itc.bean.Student;
import com.typesafe.config.ConfigException.Null;

/**
 * @author Hu.Yue
 *
 */
public class toKafka {

	
	private static Logger logger = Logger.getLogger(toKafka.class);
	private KafkaProducer<String, String> producer;
	private Properties properties;
	
	public static final String broker_list = "node01:9092";
    public static final String topic = "student";
	
	/**
	 * @throws InterruptedException   
	* @Title: main  
	* @Description: Main
	* @param args
	* @return void
	* @throws  
	*/
	public static void main(String[] args) throws InterruptedException {
		writeToKafka();
	}
	
	public static void writeToKafka() throws InterruptedException {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "node01:9092");
		//序列化
		properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		properties.put("acks", "all");
		properties.put("retries", 3);
		properties.put("batch.size", 65536);
		properties.put("linger.ms", 1);
		properties.put("buffer.memory", 33554432);
		properties.put("max.request.size", 10485760);
        
        
        KafkaProducer producer = new KafkaProducer<String, String>(properties);

        for (int i = 1; i <= 100; i++) {
            Student student = new Student(i, "huyue" + i, "password" + i, 18 + i);
            ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic,"huyue",JSON.toJSONString(student));
            producer.send(record);
            System.out.println("发送数据: " + JSON.toJSONString(student));
        }
        producer.flush();
        producer.close();
    }


}
