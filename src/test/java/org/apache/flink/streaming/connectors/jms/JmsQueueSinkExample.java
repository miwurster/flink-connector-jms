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
import javax.jms.Session;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class JmsQueueSinkExample
{
  public static void main(String[] args) throws Exception
  {
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.fromElements(new Tuple2<>("Hello", 1), new Tuple2<>("World", 2))
      .addSink(new JmsQueueSinkImpl());
    env.execute();
  }

  private static class JmsQueueSinkImpl extends JmsQueueSink<Tuple2<String, Integer>>
  {
    private static final long serialVersionUID = 42L;

    public JmsQueueSinkImpl()
    {
      super(new ActiveMQConnectionFactory("failover:tcp://192.168.99.100:61617"),
            new ActiveMQQueue("FLINK_QUEUE"));
    }

    @Override
    protected Message convert(final Tuple2<String, Integer> tupel, final Session session) throws Exception
    {
      return session.createTextMessage(tupel.f0 + " [" + tupel.f1 + "]");
    }
  }
}
