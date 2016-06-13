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

package org.apache.flink.streaming.connectors.jms;

import javax.jms.JMSException;
import javax.jms.Queue;
import javax.jms.QueueConnection;
import javax.jms.QueueConnectionFactory;
import javax.jms.QueueReceiver;
import javax.jms.QueueSession;
import javax.jms.Session;
import org.apache.flink.api.common.functions.StoppableFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jms.support.JmsUtils;
import org.springframework.jms.support.converter.MessageConverter;
import org.springframework.jms.support.converter.SimpleMessageConverter;
import org.springframework.util.Assert;

public class QueueSource extends RichParallelSourceFunction<Object> implements StoppableFunction
{
  private static final long serialVersionUID = 42L;

  private static Logger logger = LoggerFactory.getLogger(QueueSource.class);

  private volatile boolean isRunning = true;

  private final Queue destination;
  private final QueueConnectionFactory connectionFactory;
  private final String messageSelector;
  private final MessageConverter messageConverter;

  private QueueConnection connection;

  // --------------------------------------------------------------------------

  public QueueSource(final QueueConnectionFactory connectionFactory, final Queue destination)
  {
    this(connectionFactory, destination, null, null);
  }

  public QueueSource(final QueueConnectionFactory connectionFactory, final Queue destination, final String messageSelector)
  {
    this(connectionFactory, destination, messageSelector, null);
  }

  public QueueSource(final QueueConnectionFactory connectionFactory, final Queue destination, final MessageConverter messageConverter)
  {
    this(connectionFactory, destination, null, messageConverter);
  }

  public QueueSource(final QueueConnectionFactory connectionFactory, final Queue destination,
                     final String messageSelector, final MessageConverter messageConverter)
  {
    Assert.notNull(connectionFactory, "QueueConnectionFactory must not be null");
    Assert.notNull(destination, "Queue must not be null");
    this.connectionFactory = connectionFactory;
    this.destination = destination;
    this.messageSelector = messageSelector;
    if (messageConverter == null) this.messageConverter = new SimpleMessageConverter();
    else this.messageConverter = messageConverter;
  }

  @Override
  public void open(final Configuration parameters) throws Exception
  {
    super.open(parameters);
    final String username = parameters.getString("jms_username", null);
    final String password = parameters.getString("jms_password", null);
    connection = connectionFactory.createQueueConnection(username, password);
    final String clientId = parameters.getString("jms_client_id", null);
    if (clientId != null) connection.setClientID(clientId);
    connection.start();
  }

  @Override
  public void run(final SourceContext<Object> context) throws Exception
  {
    while (isRunning)
    {
      QueueSession session = null;
      QueueReceiver consumer = null;
      try
      {
        session = connection.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
        if (messageSelector != null) consumer = session.createReceiver(destination, messageSelector);
        else consumer = session.createReceiver(destination);
        context.collect(messageConverter.fromMessage(consumer.receive()));
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
