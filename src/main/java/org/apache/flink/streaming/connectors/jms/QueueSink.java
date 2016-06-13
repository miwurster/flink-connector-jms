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
import org.springframework.jms.support.JmsUtils;
import org.springframework.jms.support.converter.MappingJackson2MessageConverter;
import org.springframework.jms.support.converter.MessageConversionException;
import org.springframework.jms.support.converter.MessageConverter;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;

public class QueueSink extends RichSinkFunction<Object>
{
  private static final long serialVersionUID = 42L;

  private static Logger logger = LoggerFactory.getLogger(QueueSink.class);

  private final Queue destination;
  private final QueueConnectionFactory connectionFactory;
  private final MessageConverter messageConverter;

  private QueueConnection connection;

  // --------------------------------------------------------------------------

  public QueueSink(final QueueConnectionFactory connectionFactory, final Queue destination)
  {
    this(connectionFactory, destination, null);
  }

  public QueueSink(final QueueConnectionFactory connectionFactory, final Queue destination, final MessageConverter messageConverter)
  {
    Assert.notNull(connectionFactory, "QueueConnectionFactory must not be null");
    Assert.notNull(destination, "Queue must not be null");
    this.connectionFactory = connectionFactory;
    this.destination = destination;
    if (messageConverter == null) this.messageConverter = new MappingJackson2MessageConverter();
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
  public void invoke(final Object object) throws Exception
  {
    QueueSession session = null;
    QueueSender producer = null;
    try
    {
      session = connection.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
      producer = session.createSender(destination);
      producer.send(destination,
                    messageConverter.toMessage(object, session),
                    Message.DEFAULT_DELIVERY_MODE,
                    Message.DEFAULT_PRIORITY,
                    Message.DEFAULT_TIME_TO_LIVE);
    }
    catch (MessageConversionException e)
    {
      logger.error("Error converting object of type [{}] into a message: {}", ObjectUtils.nullSafeClassName(object), e.getLocalizedMessage(), e);
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
