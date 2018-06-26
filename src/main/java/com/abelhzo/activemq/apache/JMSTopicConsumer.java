package com.abelhzo.activemq.apache;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.ObjectMessage;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.command.ActiveMQBytesMessage;

import com.abelhzo.activemq.dto.InfoJmsDTO;

public class JMSTopicConsumer implements Runnable, MessageListener {
	
	private ActiveMQConnectionFactory connectionFactory;

	public JMSTopicConsumer(ActiveMQConnectionFactory connectionFactory) {
		this.connectionFactory = connectionFactory;
	}

	public void run() {
		
		Connection connection = null;
		Session session = null;
		
		try {
			connection = connectionFactory.createConnection();
			connection.start();
			
			session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
			
			Destination destination = session.createTopic("AbelHZOTopic");
			
			MessageConsumer messageConsumer = session.createConsumer(destination);
			
			System.out.println("Escuchando llegada de mensajes ... )))");
			System.out.println();
			messageConsumer.setMessageListener(this);
			
			try {
				Thread.sleep(25000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			System.out.println();
			System.out.println("Se cierra la escucha </>");
			
		} catch (JMSException e) {
			e.printStackTrace();
		} finally {
			try {
				session.close();
				connection.close();
			} catch (JMSException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}

	}

	public void onMessage(Message message) {

//		TextMessage textMessage = (TextMessage) message; 
		try {
			
			if(message instanceof ObjectMessage) {
				
				ObjectMessage objectMessage = (ObjectMessage) message;
				InfoJmsDTO infoJmsDTO = (InfoJmsDTO) objectMessage.getObject();
				
				System.out.println("ID: " + infoJmsDTO.getKey());
				System.out.println("Nombre: " + infoJmsDTO.getName());
				System.out.println("Fecha: " + infoJmsDTO.getDate());
				System.out.println("Tipo JMS: " + infoJmsDTO.getTypeJms());
				System.out.println("---------------------------------------------------");
			
			} else if(message instanceof ActiveMQBytesMessage) {  //Cuando el mensaje viene como un XML del server (Jaxb2Marshaller)
				
				ActiveMQBytesMessage activeMQBytesMessage = (ActiveMQBytesMessage) message;
				byte[] data = activeMQBytesMessage.getContent().getData();
				System.out.println(new String(data));
				
			} else { 	//Imprime tal cual el xml que genero esta aplicación
				
				TextMessage textMessage = (TextMessage) message;
				System.out.println(textMessage.getText());
				
			}
			
		} catch (JMSException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

}
