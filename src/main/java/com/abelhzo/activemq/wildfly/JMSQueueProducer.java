package com.abelhzo.activemq.wildfly;

import java.io.StringWriter;
import java.util.Date;
import java.util.Properties;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.ObjectMessage;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;

import com.abelhzo.activemq.dto.InfoJmsDTO;

public class JMSQueueProducer {
	
	private Properties properties;

	public JMSQueueProducer(Properties properties) {
		this.properties = properties;
	}

	public void sendQueue(int key, String typeSend) {
		
		Connection connection = null;
		Session session = null;
		
		try {
			Context context = new InitialContext(properties);
			Queue queue = (Queue) context.lookup(properties.getProperty("destination"));
			ConnectionFactory connectionFactory = (ConnectionFactory) context.lookup("jms/RemoteConnectionFactory");
			connection = connectionFactory.createConnection(properties.getProperty("user"), properties.getProperty("pass"));
			connection.start();
			
			session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
			
			MessageProducer messageProducer = session.createProducer(queue);
			
			InfoJmsDTO infoJmsDTO = new InfoJmsDTO();
			infoJmsDTO.setKey(key);
			infoJmsDTO.setName("Queue_" + key);
			infoJmsDTO.setDate(new Date());
			infoJmsDTO.setTypeJms("Queue");
			
			StringWriter sw = new StringWriter();
			try {
				JAXBContext jaxbContext = JAXBContext.newInstance(InfoJmsDTO.class);
				Marshaller createMarshaller = jaxbContext.createMarshaller();
				createMarshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, Boolean.TRUE);
				createMarshaller.marshal(infoJmsDTO, sw);
			} catch (JAXBException e) {
				e.printStackTrace();
			}
			
			if(typeSend.equals("XML")) {
				TextMessage textMessage = session.createTextMessage(sw.toString());
				messageProducer.send(textMessage);
			} else
			if(typeSend.equals("OBJ")) {
				ObjectMessage objectMessage = session.createObjectMessage(infoJmsDTO);
				messageProducer.send(objectMessage);
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
