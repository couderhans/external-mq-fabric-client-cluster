package org.fusebyexample.mq_fabric_client.simple;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Session;
import javax.jms.TextMessage;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class SimpleConsumerThread extends Thread {

	private static final Logger LOG = LoggerFactory.getLogger(SimpleConsumerThread.class);
	private static final Boolean NON_TRANSACTED = false;
	private static final String DESTINATION_NAME = "queue/simple";
	private static final int MESSAGE_TIMEOUT_MILLISECONDS = 120000;

	Connection connection;
	Session session;
	MessageConsumer consumer;
	Destination destination;

	SimpleConsumerThread(Connection connectionThread) throws JMSException {
		connection = connectionThread;
		this.setName("Thread-" + connection.getClientID().toString());
		this.start();
	}

	@Override
	public void run() {

		// JNDI lookup of JMS Connection Factory and JMS Destination
		Context context;

		try {
			context = new InitialContext();

			connection.start();

			session = connection.createSession(NON_TRANSACTED,Session.AUTO_ACKNOWLEDGE);
			destination = session.createQueue("Consumer." + connection.getClientID().toString() + ".VirtualTopic.fabric.simple");
			
			MessageConsumer consumer = session.createConsumer(destination);

			LOG.info("Consumer: " + connection.getClientID().toString()
					+ " starts consuming messages from {} with {}ms timeout",
					destination, MESSAGE_TIMEOUT_MILLISECONDS);

			// Synchronous message consumer
			int i = 1;
			while (true) {
				Message message = consumer.receive(MESSAGE_TIMEOUT_MILLISECONDS);
				if (message != null) {
					if (message instanceof TextMessage) {
						String text = ((TextMessage) message).getText();
						LOG.info("Got {}. message: {}", i++, text);
					}
				} else {
					break;
				}
			}
		} catch (NamingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (JMSException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
	
	public void done() throws JMSException {
		consumer.close();
		session.close();
        if (connection != null) {
            try {
                connection.close();
            } catch (JMSException e) {
                LOG.error("Error closing connection", e);
            }
        } 
    }
	


}
