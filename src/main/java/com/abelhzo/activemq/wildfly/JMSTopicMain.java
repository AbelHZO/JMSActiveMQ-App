package com.abelhzo.activemq.wildfly;

import java.util.Properties;

import javax.naming.Context;

public class JMSTopicMain {
	
	public static void main(String[] args) throws InterruptedException { 
		
		/**
		* El usuario y password debe de ser creado con el user-add.bat en ApplicationUser con el rol "guest" 
		* o "UserTopicJMS" asignado en la configuracion de ActiveMQ del archivo standalone-full.xml 
		* 
		* Los usuario y roles se guandan en:
		* 
		* C:\wildfly-11.0.0.Final\standalone\configuration\application-roles.properties
		* C:\wildfly-11.0.0.Final\standalone\configuration\application-users.properties
		* 
		* C:\wildfly-11.0.0.Final\domain\configuration\application-roles.properties
		* C:\wildfly-11.0.0.Final\domain\configuration\application-users.properties
		* 
		* NOTA: Para utilizar WildFlyInitialContextFactory y los paquetes prefixes de jboss
		* se agrego el jar del jboss-client.jar que viene en la carpeta bin del server.
		* No se agrega atravez del maven sino a travez del Java Build Path de Eclipse.
		*
		* Para proyectos Maven se puede agregar la siguiente dependencia, según sea la versión del
		* wildfly:
		*
		*		<dependency>
		*			<groupId>org.wildfly</groupId>
		*			<artifactId>wildfly-jms-client-bom</artifactId>
		*			<version>11.0.0.Final</version>
		*			<type>pom</type>
		*		</dependency>
		* 
		*/
		
		Properties properties = new Properties();
		properties.put(Context.URL_PKG_PREFIXES, "org.jboss.ejb.client.naming");
		properties.put("jboss.naming.client.ejb.context", true);
		properties.put("user", "JMSUserTopic");
		properties.put("pass", "topic123#"); //ROLE:   JMSUser=UserTopicJMS
		properties.put("destination", "jms/AbelHZOTopic");
		properties.put(Context.INITIAL_CONTEXT_FACTORY, "org.wildfly.naming.client.WildFlyInitialContextFactory");
		properties.put(Context.PROVIDER_URL, "http-remoting://127.0.0.1:8080");	
		
		Thread threadTopicConsumer = new Thread(new JMSTopicConsumer(properties));
		threadTopicConsumer.start();
		
		/**
		 * Comentar y descomentar para enviar un xml o un objecto.
		 */
		String typeSend = "XML";
//		String typeSend = "OBJ";
		JMSTopicProducer producerTopic = new JMSTopicProducer(properties);
		for(int i = 1; i <= 15; i++) {
			producerTopic.sendTopic(i, typeSend);
			Thread.sleep(1000);
		}
		
	}

}
