package org.fusebyexample.mq_fabric_client.simple;

import javax.jms.*;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;

import org.apache.activemq.broker.region.virtual.VirtualTopic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class SimpleProducerThread extends Thread {
	
    private static final Logger LOG = LoggerFactory.getLogger(SimpleProducerThread.class);
	
    private static final Boolean NON_TRANSACTED = false;
    private static final int MESSAGE_DELAY_MILLISECONDS = 500;
    private static final long MESSAGE_TIME_TO_LIVE_MILLISECONDS = 0;

	private static final String DESTINATION_NAME = "topic/simple";
	
	Connection connection;
	Session session;
	MessageProducer producer;
	
	SimpleProducerThread(Connection connectionThreaded) throws JMSException {
		connection = connectionThreaded;
		this.setName("Thread-" + connection.getClientID().toString());
		this.start();		
	}

	@Override
	public void run() {
		
        // JNDI lookup of JMS Connection Factory and JMS Destination
        Context context;
        
		try {
			context = new InitialContext();
	        //Destination destination = (Destination) context.lookup(DESTINATION_NAME); 
	        
			connection.start();

	        session = connection.createSession(NON_TRANSACTED, Session.AUTO_ACKNOWLEDGE);

	        VirtualTopic topic = (VirtualTopic) session.createTopic("test");
	        topic.setSelectorAware(true);
	        Destination destination = topic.getVirtualDestination();
	        Producer producer = session.createProducer(destination);
	        
	        producer.setTimeToLive(MESSAGE_TIME_TO_LIVE_MILLISECONDS);

	        for (int i = 1; i <= 10; i++) {
	            TextMessage message = session.createTextMessage("Producer thread: " + this.getName() + " msg: " + i + ". message sent from producer: " + connection.getClientID());
	            LOG.info("Sending to destination: {} this text: {}", destination, message.getText());
	            producer.send(message);
	            Thread.sleep(MESSAGE_DELAY_MILLISECONDS);
	        }    
	        
	        
		} catch (NamingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (JMSException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		
	}
	
	public void done() throws JMSException {
        // Cleanup
        producer.close();
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
