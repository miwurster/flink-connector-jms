package org.apache.flink.streaming.connectors.activemq;

import org.apache.flink.api.common.functions.StoppableFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ActiveMQQueueSource extends RichParallelSourceFunction<String> implements StoppableFunction
{
  private static final long serialVersionUID = 42L;

  private static Logger logger = LoggerFactory.getLogger(ActiveMQQueueSource.class);

  private volatile boolean isRunning = true;

  public ActiveMQQueueSource()
  {

  }

  @Override
  public void open(final Configuration parameters) throws Exception
  {
    super.open(parameters);
    // TODO: Request resources
  }

  @Override
  public void run(final SourceContext<String> sourceContext) throws Exception
  {
    while (isRunning)
    {
      // TODO: Do your actions
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
    // TODO: Close resources
  }
}
