package org.infinispan.util.concurrent;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;

import org.infinispan.manager.CacheContainer;
import org.infinispan.test.TestingUtil;

public class BlockingManagerTestUtil {
   /**
    * Replaces the cache container's {@link BlockingManager} and {@link NonBlockingManager} components with ones that
    * runs all blocking and non blocking operations in the invoking thread. This is useful for testing when  you want
    * operations to be done sequentially.
    * <p>
    * This operation will be undone if component registry is rewired
    *
    * @param cacheContainer Container of which the blocking manager is to be replaced
    */
   public static void replaceManagersWithInline(CacheContainer cacheContainer) {
      NonBlockingManagerImpl nonBlockingManager = (NonBlockingManagerImpl) TestingUtil.extractGlobalComponent(cacheContainer, NonBlockingManager.class);
      nonBlockingManager.executor = new WithinThreadExecutor();

      BlockingManagerImpl manager = (BlockingManagerImpl) TestingUtil.extractGlobalComponent(cacheContainer, BlockingManager.class);
      manager.blockingExecutor = new WithinThreadExecutor();
      manager.nonBlockingExecutor = new WithinThreadExecutor();
      manager.nonBlockingManager = nonBlockingManager;
   }

   /**
    * Replaces the container's blocking executor with one that queues every submitted task instead of
    * running it, so blocking operations (for example an asynchronous cache start) can be held for the
    * duration of a test. Call {@link HeldBlockingExecutor#release()} to run the queued tasks and
    * resume normal execution.
    *
    * @param cacheContainer the container whose blocking executor is to be held.
    * @return the handle used to release the held tasks.
    */
   public static HeldBlockingExecutor holdBlockingExecutor(CacheContainer cacheContainer) {
      BlockingManagerImpl manager = (BlockingManagerImpl) TestingUtil.extractGlobalComponent(cacheContainer, BlockingManager.class);
      HeldBlockingExecutor held = new HeldBlockingExecutor(manager.blockingExecutor);
      manager.blockingExecutor = held;
      return held;
   }

   /**
    * An {@link Executor} that queues submitted tasks until {@link #release()} is called, after which
    * queued and subsequently submitted tasks run on the original executor.
    */
   public static final class HeldBlockingExecutor implements Executor {
      private final Executor delegate;
      private final Queue<Runnable> held = new ConcurrentLinkedQueue<>();
      private volatile boolean holding = true;

      private HeldBlockingExecutor(Executor delegate) {
         this.delegate = delegate;
      }

      @Override
      public void execute(Runnable command) {
         if (holding) {
            held.add(command);
         } else {
            delegate.execute(command);
         }
      }

      /**
       * Stops holding tasks and runs everything queued so far on the original executor.
       */
      public void release() {
         holding = false;
         for (Runnable command; (command = held.poll()) != null; ) {
            delegate.execute(command);
         }
      }
   }
}
