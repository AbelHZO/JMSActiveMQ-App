package com.abelhzo.activemq.wildfly;

import java.io.StringWriter;
import java.util.Date;
import java.util.Properties;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.DeliveryMode;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.ObjectMessage;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;

import com.abelhzo.activemq.dto.InfoJmsDTO;

public class JMSTopicProducer {
	
	private Properties properties;

	public JMSTopicProducer(Properties properties) {
		this.properties = properties;
	}

	public void sendTopic(int key, String typeSend) {
		
		Connection connection = null;
		Session session = null;
		
		try {
			Context context = new InitialContext(properties);
			Topic topic = (Topic) context.lookup(properties.getProperty("destination"));
			ConnectionFactory connectionFactory = (ConnectionFactory) context.lookup("jms/RemoteConnectionFactory");
			connection = connectionFactory.createConnection(properties.getProperty("user"), properties.getProperty("pass"));
			connection.start();
			
			session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
			
//			Destination destination = session.createTopic("CoordinatesQueue");
//			MessageProducer messageProducer = session.createProducer(destination);
//			messageProducer.setDeliveryMode(DeliveryMode.PERSISTENT);
			
			MessageProducer messageProducer = session.createProducer(topic);
			messageProducer.setDeliveryMode(DeliveryMode.PERSISTENT);
			
			InfoJmsDTO infoJmsDTO = new InfoJmsDTO();
			infoJmsDTO.setKey(key);
			infoJmsDTO.setName("Topic_" + key);
			infoJmsDTO.setDate(new Date());
			infoJmsDTO.setTypeJms("Topic");
			
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
