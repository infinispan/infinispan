package org.infinispan.spring.mock;

import java.util.Properties;
import java.util.concurrent.ScheduledExecutorService;

import org.infinispan.executors.ScheduledExecutorFactory;

public final class MockScheduleExecutorFactory implements ScheduledExecutorFactory {

   @Override
   public ScheduledExecutorService getScheduledExecutor(final Properties p) {
      return null;
   }
}
