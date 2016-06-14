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
import javax.jms.Message;
import javax.jms.Queue;
import javax.jms.QueueConnection;
import javax.jms.QueueConnectionFactory;
import javax.jms.QueueSender;
import javax.jms.QueueSession;
import javax.jms.Session;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.jms.UncategorizedJmsException;
import org.springframework.jms.support.JmsUtils;
import org.springframework.util.Assert;

public abstract class JmsQueueSink<T> extends RichSinkFunction<T> implements InitializingBean
{
  private static final long serialVersionUID = 42L;

  private static Logger logger = LoggerFactory.getLogger(JmsQueueSink.class);

  private Queue destination;
  private QueueConnectionFactory connectionFactory;

  private QueueConnection connection;
  private QueueSession session;
  private QueueSender producer;

  // --------------------------------------------------------------------------

  public JmsQueueSink()
  {

  }

  public JmsQueueSink(final QueueConnectionFactory connectionFactory, final Queue destination)
  {
    Assert.notNull(connectionFactory, "QueueConnectionFactory must not be null");
    Assert.notNull(destination, "Queue must not be null");
    this.connectionFactory = connectionFactory;
    this.destination = destination;
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
    session = connection.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
    producer = session.createSender(destination);
    connection.start();
  }

  @Override
  public void invoke(final T object) throws Exception
  {
    try
    {
      producer.send(destination, convert(object, session),
                    Message.DEFAULT_DELIVERY_MODE, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
    }
    catch (JMSException e)
    {
      logger.error("Error sending message to [{}]: {}", destination.getQueueName(), e.getLocalizedMessage());
      throw new UncategorizedJmsException(e);
    }
  }

  protected abstract Message convert(final T object, final Session session) throws Exception;

  @Override
  public void close() throws Exception
  {
    super.close();
    JmsUtils.closeMessageProducer(producer);
    JmsUtils.closeSession(session);
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

  @Override
  public void afterPropertiesSet() throws Exception
  {
    Assert.notNull(destination, "Destination queue must not be null");
    Assert.notNull(connectionFactory, "QueueConnectionFactory must not be null");
  }
}
