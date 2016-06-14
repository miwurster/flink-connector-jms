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

import javax.annotation.Nullable;
import javax.jms.JMSException;
import javax.jms.Message;
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
import org.springframework.beans.factory.InitializingBean;
import org.springframework.jms.UncategorizedJmsException;
import org.springframework.jms.support.JmsUtils;
import org.springframework.util.Assert;

public abstract class JmsQueueSource<T> extends RichParallelSourceFunction<T> implements StoppableFunction, InitializingBean
{
  private static final long serialVersionUID = 42L;

  private static Logger logger = LoggerFactory.getLogger(JmsQueueSource.class);

  private volatile boolean isRunning = true;

  private Queue destination;
  private QueueConnectionFactory connectionFactory;
  private String messageSelector;

  private QueueConnection connection;

  // --------------------------------------------------------------------------

  public JmsQueueSource()
  {

  }

  public JmsQueueSource(final QueueConnectionFactory connectionFactory, final Queue destination)
  {
    this(connectionFactory, destination, null);
  }

  public JmsQueueSource(final QueueConnectionFactory connectionFactory, final Queue destination, @Nullable final String messageSelector)
  {
    Assert.notNull(connectionFactory, "QueueConnectionFactory must not be null");
    Assert.notNull(destination, "Queue must not be null");
    this.connectionFactory = connectionFactory;
    this.destination = destination;
    this.messageSelector = messageSelector;
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
  }

  @Override
  public void run(final SourceContext<T> context) throws Exception
  {
    QueueSession session = null;
    QueueReceiver consumer = null;

    try
    {
      session = connection.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
      consumer = session.createReceiver(destination, messageSelector);

      connection.start();

      while (isRunning)
      {
        context.collect(convert(consumer.receive()));
      }
    }
    catch (JMSException e)
    {
      logger.error("Error receiving message from [{}]: {}", destination.getQueueName(), e.getLocalizedMessage());
      throw new UncategorizedJmsException(e);
    }
    finally
    {
      JmsUtils.closeMessageConsumer(consumer);
      JmsUtils.closeSession(session);
    }
  }

  protected abstract T convert(final Message message) throws Exception;

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

  public void setDestination(final Queue destination)
  {
    this.destination = destination;
  }

  public void setConnectionFactory(final QueueConnectionFactory connectionFactory)
  {
    this.connectionFactory = connectionFactory;
  }

  public void setMessageSelector(final String messageSelector)
  {
    this.messageSelector = messageSelector;
  }

  @Override
  public void afterPropertiesSet() throws Exception
  {
    Assert.notNull(destination, "Destination queue must not be null");
    Assert.notNull(connectionFactory, "QueueConnectionFactory must not be null");
  }
}
