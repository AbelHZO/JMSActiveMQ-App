package com.abelhzo.activemq.apache;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.ObjectMessage;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.command.ActiveMQBytesMessage;

import com.abelhzo.activemq.dto.InfoJmsDTO;

public class JMSQueueConsumer {
	
	private ActiveMQConnectionFactory connectionFactory;

	public JMSQueueConsumer(ActiveMQConnectionFactory connectionFactory) {
		this.connectionFactory = connectionFactory;
	}

	public void consumeQueue() {
		
		Connection connection = null;
		Session session = null;
		
		try {
			connection = connectionFactory.createConnection();
			connection.start();
			
			session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
			
			Destination destination = session.createQueue("AbelHZOQueue");
			
			MessageConsumer messageConsumer = session.createConsumer(destination);
			
			Message message = messageConsumer.receive();
			
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

}
