package org.infinispan.spring.mock;

import java.util.Properties;
import java.util.concurrent.ExecutorService;

import org.infinispan.commons.executors.ExecutorFactory;

public final class MockExecutorFatory implements ExecutorFactory {

   @Override
   public ExecutorService getExecutor(final Properties p) {
      return null;
   }

}
