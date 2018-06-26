package com.abelhzo.activemq.apache;

import java.util.Arrays;

import org.apache.activemq.ActiveMQConnectionFactory;

import com.abelhzo.activemq.apache.JMSQueueConsumer;
import com.abelhzo.activemq.apache.JMSQueueProducer;

public class JMSQueueMain {
	
	public static void main(String[] args) throws InterruptedException {
		
		/**
		 * Para las clases de JMS se utiliza la dependencia en el maven:
		 * 
		 * 	<dependency>
		 *		<groupId>org.apache.activemq</groupId>
		 *		<artifactId>activemq-all</artifactId>
		 *		<version>5.15.4</version>
		 *	</dependency>
		 *
		 * Que es la sugerida por la pagina de ActiveMQ
		 */
		
		ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory("tcp://127.0.0.1:61616");
		connectionFactory.setTrustedPackages(Arrays.asList("java.lang,java.util,com.abelhzo.activemq.dto".split(",")));
		
		/**
		 * Comentar y descomentar para enviar un xml o un objecto.
		 */
		String typeSend = "XML";
//		String typeSend = "OBJ";
		JMSQueueProducer producerQueue = new JMSQueueProducer(connectionFactory);
		for(int i = 1; i <= 15; i++) {
			producerQueue.sendQueue(i, typeSend);
			System.out.println("Insertando dato con key: " + i );
			Thread.sleep(1000);
		}
		
		System.out.println();
		System.out.println("Listo para recuperar: ");
		System.out.println();
		Thread.sleep(2000);
		
		JMSQueueConsumer consumerQueue = new JMSQueueConsumer(connectionFactory);
		for(int i = 1; i <= 15; i++) {
			consumerQueue.consumeQueue();
			Thread.sleep(1000);
		}
	}

}
