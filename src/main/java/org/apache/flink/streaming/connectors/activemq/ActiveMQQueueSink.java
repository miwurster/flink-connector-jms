/*
 * Copyright 2016 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.activemq;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jms.support.JmsUtils;
import org.springframework.util.Assert;

public class ActiveMQQueueSink extends RichSinkFunction<String>
{
  private static final long serialVersionUID = 42L;

  private static Logger logger = LoggerFactory.getLogger(ActiveMQQueueSink.class);

  private volatile boolean isRunning = true;

  private final ConnectionFactory connectionFactory;
  private final Queue destination;

  private Connection connection;

  // --------------------------------------------------------------------------

  public ActiveMQQueueSink(final ConnectionFactory connectionFactory, final Queue destination)
  {
    Assert.notNull(connectionFactory, "ConnectionFactory object must not be null");
    Assert.notNull(destination, "Destination queue object must not be null");
    this.connectionFactory = connectionFactory;
    this.destination = destination;
  }

  @Override
  public void open(final Configuration parameters) throws Exception
  {
    super.open(parameters);
    final String username = parameters.getString("activemq_username", null);
    final String password = parameters.getString("activemq_password", null);
    connection = connectionFactory.createConnection(username, password);
    final String clientId = parameters.getString("activemq_client_id", null);
    if (clientId != null) connection.setClientID(clientId);
    connection.start();
  }

  @Override
  public void invoke(final String value) throws Exception
  {
    Session session = null;
    MessageProducer producer = null;
    try
    {
      session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      producer = session.createProducer(destination);
      producer.send(destination,
                    session.createTextMessage(value),
                    Message.DEFAULT_DELIVERY_MODE,
                    Message.DEFAULT_PRIORITY,
                    Message.DEFAULT_TIME_TO_LIVE);
    }
    catch (JMSException e)
    {
      logger.error("Error sending message to [{}]: {}", destination.getQueueName(), e.getLocalizedMessage(), e);
    }
    finally
    {
      JmsUtils.closeMessageProducer(producer);
      JmsUtils.closeSession(session);
    }
  }

  @Override
  public void close() throws Exception
  {
    super.close();
    JmsUtils.closeConnection(connection, true);
  }
}
