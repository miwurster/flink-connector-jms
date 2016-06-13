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
import javax.jms.MessageConsumer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import org.apache.flink.api.common.functions.StoppableFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jms.support.JmsUtils;
import org.springframework.util.Assert;

public class ActiveMQQueueSource extends RichParallelSourceFunction<String> implements StoppableFunction
{
  private static final long serialVersionUID = 42L;

  private static Logger logger = LoggerFactory.getLogger(ActiveMQQueueSource.class);

  private volatile boolean isRunning = true;

  private final ConnectionFactory connectionFactory;
  private final Queue destination;
  private final String messageSelector;

  private Connection connection;

  // --------------------------------------------------------------------------

  public ActiveMQQueueSource(final ConnectionFactory connectionFactory, final Queue destination)
  {
    this(connectionFactory, destination, null);
  }

  public ActiveMQQueueSource(final ConnectionFactory connectionFactory, final Queue destination, final String messageSelector)
  {
    Assert.notNull(connectionFactory, "ConnectionFactory object must not be null");
    Assert.notNull(destination, "Destination queue object must not be null");
    this.connectionFactory = connectionFactory;
    this.destination = destination;
    this.messageSelector = messageSelector;
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
  public void run(final SourceContext<String> context) throws Exception
  {
    while (isRunning)
    {
      Session session = null;
      MessageConsumer consumer = null;
      try
      {
        session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        if (messageSelector != null) consumer = session.createConsumer(destination, messageSelector);
        else consumer = session.createConsumer(destination);

        final Message message = consumer.receive();
        if (message instanceof TextMessage) context.collect(((TextMessage) message).getText());
        else logger.info("Received unsupported message type [{}], ignoring message...",
                         message.getClass().getSimpleName());
      }
      catch (JMSException e)
      {
        logger.error("Error receiving message from [{}]: {}", destination.getQueueName(), e.getLocalizedMessage(), e);
      }
      finally
      {
        JmsUtils.closeMessageConsumer(consumer);
        JmsUtils.closeSession(session);
      }
    }
  }

  @Override
  public void cancel()
  {
    isRunning = false;
  }

  @Override
  public void stop()
  {
    isRunning = false;
  }

  @Override
  public void close() throws Exception
  {
    super.close();
    JmsUtils.closeConnection(connection, true);
  }
}