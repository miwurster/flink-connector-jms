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

import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.TextMessage;
import org.apache.flink.api.common.functions.StoppableFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.util.Assert;

public class JmsTemplateSource extends RichParallelSourceFunction<String> implements StoppableFunction
{
  private static final long serialVersionUID = 42L;

  private static Logger logger = LoggerFactory.getLogger(JmsTemplateSource.class);

  private volatile boolean isRunning = true;

  private final JmsTemplate jmsTemplate;
  private final Destination destination;
  private final String messageSelector;

  // --------------------------------------------------------------------------

  public JmsTemplateSource(final JmsTemplate jmsTemplate, final Destination destination)
  {
    this(jmsTemplate, destination, null);
  }

  public JmsTemplateSource(final JmsTemplate jmsTemplate, final Destination destination, final String messageSelector)
  {
    Assert.notNull(jmsTemplate, "JmsTemplate object must not be null");
    Assert.notNull(destination, "Destination object must not be null");
    this.jmsTemplate = jmsTemplate;
    this.destination = destination;
    this.messageSelector = messageSelector;
  }

  @Override
  public void run(final SourceContext<String> context) throws Exception
  {
    while (isRunning)
    {
      try
      {
        final Message message = jmsTemplate.receiveSelected(destination, messageSelector);
        if (message instanceof TextMessage) context.collect(((TextMessage) message).getText());
        else logger.info("Received unsupported message type [{}], ignoring message...",
                         message.getClass().getSimpleName());
      }
      catch (JMSException e)
      {
        logger.error("Error receiving message from [{}]: {}", destination.toString(), e.getLocalizedMessage(), e);
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
}
