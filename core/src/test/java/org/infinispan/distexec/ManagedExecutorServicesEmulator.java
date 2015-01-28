package org.infinispan.distexec;

import java.util.List;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * An executor that emulates the behaviour of an EE ManagedServiceExecutor, i.e. one which returns {@link IllegalStateException} on all lifecycle methods according to the spec
 *
 */
public final class ManagedExecutorServicesEmulator extends AbstractExecutorService {
   @Override
   public void execute(Runnable command) {
      command.run();
   }

   @Override
   public void shutdown() {
      throw new IllegalStateException();
   }

   @Override
   public List<Runnable> shutdownNow() {
      throw new IllegalStateException();
   }

   @Override
   public boolean isShutdown() {
      throw new IllegalStateException();
   }

   @Override
   public boolean isTerminated() {
      throw new IllegalStateException();
   }

   @Override
   public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
      throw new IllegalStateException();
   }
}
