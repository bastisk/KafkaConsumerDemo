/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package misp.demo.kafkaconsumer;

import kafka.consumer.*;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 *
 * @author basti
 */
public class MISPDemoKafkaConsumer {

	private final ConsumerConnector consumer;
	private final String topic;
	private ExecutorService executor;
	
	public MISPDemoKafkaConsumer(String a_zookeeper, String a_groupid, String a_topic) {
		consumer = kafka.consumer.Consumer.createJavaConsumerConnector(
				createConsumerConfig(a_zookeeper, a_groupid));
		this.topic = a_topic;
	}
	
	public void shutdown () {
		if (consumer != null) consumer.shutdown();
		if (executor != null) executor.shutdown();
		try {
			if (!executor.awaitTermination(5000, TimeUnit.MILLISECONDS)) {
				System.out.println("Timed out waiting for consumer threads to shut down, exiting!");
			}
		} catch (InterruptedException e) {
			System.out.println("Interrupted during shutdown, exiting!");
		}	
	}

	public void run(int a_numThreads) {
		Map<String, Integer> topicCountMap = new HashMap<>();
                Integer put = topicCountMap.put(topic, a_numThreads);
		Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
		List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);
		
		//launch the Threads
		executor = Executors.newFixedThreadPool(a_numThreads);
		
		//create an object to consume the message
		int threadNumber = 0;
		for (final KafkaStream stream : streams) {
			executor.submit(new ConsumerTest(stream, threadNumber));
			threadNumber++;
			}
	}
	
	private static ConsumerConfig createConsumerConfig(String a_zookeeper, String a_groupid) {
		Properties props = new Properties();
		props.put("zookeeper.connect", a_zookeeper);
		props.put("group.id", a_groupid);
		props.put("zookeeper.session.timeout.ms", "400");
		props.put("zookeeper.sync.time.ms", "200");
		props.put("auto.commit.interval.ms", "1000");
		
		return new ConsumerConfig(props);
	}
	
	
	public static void main(String[] args) {
		String zooKeeper = args[0];
		String groupid = args[1];
		String topic = args[2];
		int threads = Integer.parseInt(args[3]);
		
		MISPDemoKafkaConsumer example = new MISPDemoKafkaConsumer(zooKeeper, groupid, topic);
		example.run(threads);
		
		try {
			Thread.sleep(1000);
		} catch (InterruptedException ie) {
                    
		}
		example.shutdown();
		// TODO Auto-generated method stub
	}
    
}
