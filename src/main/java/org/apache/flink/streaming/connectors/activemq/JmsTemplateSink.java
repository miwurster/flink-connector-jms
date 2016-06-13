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
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jms.JmsException;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.util.Assert;

public class JmsTemplateSink<T> extends RichSinkFunction<T>
{
  private static final long serialVersionUID = 42L;

  private static Logger logger = LoggerFactory.getLogger(JmsTemplateSink.class);

  private final JmsTemplate jmsTemplate;
  private final Destination destination;

  // --------------------------------------------------------------------------

  public JmsTemplateSink(final JmsTemplate jmsTemplate, final Destination destination)
  {
    Assert.notNull(jmsTemplate, "JmsTemplate object must not be null");
    Assert.notNull(destination, "Destination object must not be null");
    this.jmsTemplate = jmsTemplate;
    this.destination = destination;
  }

  @Override
  public void invoke(final T t) throws Exception
  {
    try
    {
      jmsTemplate.convertAndSend(destination, t);
    }
    catch (JmsException e)
    {
      logger.error("Error sending message to [{}]: {}", destination.toString(), e.getLocalizedMessage(), e);
    }
  }
}
