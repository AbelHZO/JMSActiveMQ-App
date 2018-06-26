package com.abelhzo.activemq.wildfly;

import java.util.Properties;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;

import org.apache.activemq.artemis.jms.client.ActiveMQBytesMessage;
import org.apache.activemq.artemis.jms.client.ActiveMQObjectMessage;

import com.abelhzo.activemq.dto.InfoJmsDTO;

public class JMSQueueConsumer {

	private Properties properties;

	public JMSQueueConsumer(Properties properties) {
		this.properties = properties;
	}

	public void consumeQueue() {

		Connection connection = null;
		Session session = null;

		try {
			Context context = new InitialContext(properties);
			Queue queue = (Queue) context.lookup(properties.getProperty("destination"));
			ConnectionFactory connectionFactory = (ConnectionFactory) context.lookup("jms/RemoteConnectionFactory");
			connection = connectionFactory.createConnection(properties.getProperty("user"), properties.getProperty("pass"));
			connection.start();
			
			session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

			MessageConsumer messageConsumer = session.createConsumer(queue);
			
			Message message = messageConsumer.receive();
			
			if(message instanceof ActiveMQObjectMessage) { //Cuando el mensaje viene como un Objecto
				
				ActiveMQObjectMessage activeMQObjectMessage = (ActiveMQObjectMessage) message;
				InfoJmsDTO infoJmsDTO = activeMQObjectMessage.getBody(InfoJmsDTO.class);
				
				System.out.println("ID: " + infoJmsDTO.getKey());
				System.out.println("Nombre: " + infoJmsDTO.getName());
				System.out.println("Fecha: " + infoJmsDTO.getDate());
				System.out.println("Tipo JMS: " + infoJmsDTO.getTypeJms());
				System.out.println("---------------------------------------------------");
				
			} else if(message instanceof ActiveMQBytesMessage) {  //Cuando el mensaje viene como un XML del server (Jaxb2Marshaller)
				
				ActiveMQBytesMessage activeMQBytesMessage = (ActiveMQBytesMessage) message;
				byte[] infoJmsDTO = activeMQBytesMessage.getBody(byte[].class);
				System.out.println(new String(infoJmsDTO));
				
			} else {   //Imprime tal cual el xml que genero esta aplicación
				
				TextMessage textMessage = (TextMessage) message;
				System.out.println(textMessage.getText());
				
			}

		} catch (NamingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (JMSException e) {
			// TODO Auto-generated catch block
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
