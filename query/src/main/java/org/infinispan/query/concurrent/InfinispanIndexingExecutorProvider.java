package org.infinispan.query.concurrent;

import java.util.concurrent.Future;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.hibernate.search.backend.lucene.work.spi.LuceneWorkExecutorProvider;
import org.hibernate.search.engine.common.execution.spi.SimpleScheduledExecutor;
import org.infinispan.factories.AbstractComponentFactory;
import org.infinispan.factories.AutoInstantiableFactory;
import org.infinispan.factories.annotations.DefaultFactoryFor;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.util.concurrent.BlockingManager;

@Scope(Scopes.GLOBAL)
@DefaultFactoryFor(classes = LuceneWorkExecutorProvider.class)
public class InfinispanIndexingExecutorProvider extends AbstractComponentFactory implements AutoInstantiableFactory, LuceneWorkExecutorProvider {

   @Inject
   BlockingManager blockingManager;

   @Override
   public Object construct(String name) {
      return new InfinispanIndexingExecutorProvider();
   }

   @Override
   public SimpleScheduledExecutor writeExecutor(Context context) {
      return InfinispanIndexingExecutorProvider.writeExecutor(blockingManager);
   }

   public static SimpleScheduledExecutor writeExecutor(BlockingManager blockingManager) {
      return new InfinispanScheduledExecutor(blockingManager);
   }

   static final class InfinispanScheduledExecutor implements SimpleScheduledExecutor {

      private final BlockingManager blockingManager;

      public InfinispanScheduledExecutor(BlockingManager blockingManager) {
         this.blockingManager = blockingManager;
      }

      @Override
      public Future<?> submit(Runnable task) {
         return blockingManager.runBlocking(task, this).toCompletableFuture();
      }

      @Override
      public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit) {
         return blockingManager.scheduleRunBlocking(command, delay, unit, this);
      }

      @Override
      public void shutdownNow() {
         // The executors lifecycle is handled at global Infinispan level
      }

      @Override
      public boolean isBlocking() {
         // If the invoking thread is non-blocking and the blocking pool and its queue are full, then it will throw an exception.
         // If the invoking thread is blocking and the blocking pool and its queue are full, then it will run the task immediately in the invoking thread.
         // In either case the invocation is not "blocking" but it may not return immediately as it is performing the task.
         return false;
      }
   }
}
