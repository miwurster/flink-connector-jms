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

import javax.jms.Message;
import javax.jms.TextMessage;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.springframework.jms.support.converter.MessageConversionException;
import org.springframework.util.ObjectUtils;

public class JmsTopicSourceExample
{
  public static void main(String[] args) throws Exception
  {
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.addSource(new JmsTopicSourceImpl())
      .print();
    env.execute();
  }

  private static class JmsTopicSourceImpl extends JmsTopicSource<String>
  {
    private static final long serialVersionUID = 42L;

    public JmsTopicSourceImpl()
    {
      super(new ActiveMQConnectionFactory("failover:tcp://192.168.99.100:61617"),
            new ActiveMQTopic("FLINK_TOPIC"));
    }

    @Override
    protected String convert(final Message object) throws Exception
    {
      if (object instanceof TextMessage)
      {
        final TextMessage message = (TextMessage) object;
        return message.getText();
      }
      throw new MessageConversionException("Cannot convert message of type [" + ObjectUtils.nullSafeClassName(object) + "]");
    }
  }
}
