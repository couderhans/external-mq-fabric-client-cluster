/*
 * Copyright (C) Red Hat, Inc.
 * http://www.redhat.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.fusebyexample.mq_fabric_client.simple;

import java.util.ArrayList;
import java.util.List;

import javax.jms.*;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;

import org.apache.activemq.pool.PooledConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SimpleConsumer {
	
    private static final Logger LOG = LoggerFactory.getLogger(SimpleConsumer.class);
    private static final String CONNECTION_FACTORY_NAME = "myJmsFactory";
	private static final String DESTINATION_NAME = "queue/simple";

    public static void main(String args[]) throws NamingException, JMSException {
        final String brokerUrl = System.getProperty("java.naming.provider.url");

        if (brokerUrl != null) {
            LOG.info("******************************");
            LOG.info("Overriding jndi brokerUrl, now using: {}", brokerUrl);
            LOG.info("******************************");
        }

    	List<Connection> connections = createPooledConnectionList(16);
		Context context = new InitialContext();
		//Destination destination = (Destination) context.lookup(DESTINATION_NAME)

		
        try {         
        	for (Connection connection: connections) {
        		
        		new SimpleConsumerThread(connection);       		
        	}
        } catch (Throwable t) {
            LOG.error("Error receiving message", t);
        }
    }
    
    private static List<Connection> createPooledConnectionList(Integer numberOfConnections) throws NamingException, JMSException {
        
    	Context context = new InitialContext();
    	List<Connection> connectionList = new ArrayList<Connection>();    	

        ConnectionFactory factory = (ConnectionFactory) context.lookup(CONNECTION_FACTORY_NAME);                   

        PooledConnectionFactory pooledConnectionFactory = new PooledConnectionFactory();
        pooledConnectionFactory.setIdleTimeout(5000);
        pooledConnectionFactory.setMaxConnections(10);
        pooledConnectionFactory.start();

        pooledConnectionFactory.setConnectionFactory(factory);

    	for (int i = 1; i <= numberOfConnections; i++) {
    		Connection connection = pooledConnectionFactory.createConnection();
    		connection.setClientID("Consumer" + i);
    		connectionList.add(connection);
    	}   	
    	
    	return connectionList;    	
    }
}
