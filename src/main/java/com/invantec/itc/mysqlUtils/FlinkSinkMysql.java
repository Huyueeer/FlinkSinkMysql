/**
* <p>Title: FlinkSinkMysql.java</p>  
* <p>Description: 消费数据sink到mysql</p>   
* @author Hu.Yue
* @date Dec 29, 2020  
* @version 1.0 
*/  


package com.invantec.itc.mysqlUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.shaded.curator4.com.google.common.collect.Lists;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import com.alibaba.fastjson.JSON;
import com.invantec.itc.bean.Student;

/**
 * @author Hu.Yue
 *
 */
public class FlinkSinkMysql {
	static String topic = "student";
	
	/**
	 * @throws Exception   
	* @Title: main  
	* @Description: TODO
	* @param @param args
	* @return void
	* @throws  
	*/
	public static void main(String[] args) throws Exception {
		Properties props = new Properties();
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		props.put("bootstrap.servers", "node01:9092");
        props.put("zookeeper.connect", "node01:2181");
        props.put("group.id", "java-group");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("auto.offset.reset", "latest");
		
		SingleOutputStreamOperator<Student> student = env.addSource(new FlinkKafkaConsumer<>(topic, new SimpleStringSchema(), props))
				.map(new MapFunction<String, Student>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Student map(String value) throws Exception {
						Student student = JSON.parseObject(value, Student.class);
						return student;
					}
				});
		
		student.timeWindowAll(Time.seconds(3)).apply(new AllWindowFunction<Student, List<Student>, TimeWindow>() {
			private static final long serialVersionUID = 1L;

			@Override
			public void apply(TimeWindow window, Iterable<Student> values, Collector<List<Student>> out)
					throws Exception {
				ArrayList<Student> students = Lists.newArrayList(values);
				if(students.size() > 0) {
					System.out.println("3秒内收集到student--" + students.size() + "条数据；");
					out.collect(students);
				}
				
			}
		})
		.addSink(new SinkToMysql());
		
		env.execute("KafkaFlinkSinkMysql");
	}

}
