package org.infinispan.executors;

import java.util.Properties;
import java.util.concurrent.ExecutorService;

import org.infinispan.commons.executors.ExecutorFactory;
import org.infinispan.util.concurrent.WithinThreadExecutor;

/**
 * Executor factory that creates WithinThreadExecutor. This executor executes the tasks in the caller thread.
 *
 * @author Pedro Ruivo
 * @since 5.3
 */
public class WithinThreadExecutorFactory implements ExecutorFactory {

   @Override
   public ExecutorService getExecutor(Properties p) {
      return new WithinThreadExecutor();
   }
}
