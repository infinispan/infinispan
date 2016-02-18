package org.infinispan.util.concurrent;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * An executor that works within the current thread.
 *
 * @author Manik Surtani (<a href="mailto:manik@jboss.org">manik@jboss.org</a>)
 * @see <a href="http://jcip.net/">Java Concurrency In Practice</a>
 * @since 4.0
 */
public final class WithinThreadExecutor extends AbstractExecutorService {
   private volatile boolean shutDown = false;

   @Override
   public void execute(Runnable command) {
      command.run();
   }

   @Override
   public void shutdown() {
      shutDown = true;
   }

   @Override
   public List<Runnable> shutdownNow() {
      shutDown = true;
      return Collections.emptyList();
   }

   @Override
   public boolean isShutdown() {
      return shutDown;
   }

   @Override
   public boolean isTerminated() {
      return shutDown;
   }

   @Override
   public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
      return shutDown;
   }
}
