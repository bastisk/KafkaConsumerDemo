/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package misp.demo.kafkaconsumer;

import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;

/**
 *
 * @author basti
 */
public class ConsumerTest implements Runnable {
    	private final KafkaStream m_stream;
	private final int m_threadNumber;
	
	public ConsumerTest(KafkaStream a_stream, int a_threadNumber) {
		m_threadNumber = a_threadNumber;
		m_stream = a_stream;
	}
	
	public void run() {
		ConsumerIterator<byte[], byte[]> it = m_stream.iterator();
		while (it.hasNext())
			System.out.println("Thread" + m_threadNumber + ": " + new String(it.next().message()));
		System.out.println("Shutting down Thread: " + m_threadNumber);
	}
    
}
