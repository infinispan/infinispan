package org.infinispan.api;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.commons.CacheException;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.VersioningScheme;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.transaction.LockingMode;
import org.infinispan.util.concurrent.IsolationLevel;
import org.infinispan.util.concurrent.locks.LockManager;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.junit.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.HashSet;
import java.util.List;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Verifies the atomic semantic of Infinispan's implementations of java.util.concurrent.ConcurrentMap'
 * conditional operations.
 *
 * @author Sanne Grinovero <sanne@infinispan.org> (C) 2012 Red Hat Inc.
 * @see java.util.concurrent.ConcurrentMap#replace(Object, Object, Object)
 * @since 5.2
 */
@Test(groups = "functional", testName = "api.ConditionalOperationsConcurrentTest")
public class ConditionalOperationsConcurrentTest extends MultipleCacheManagersTest {

   private final Log log = LogFactory.getLog(getClass());

   public ConditionalOperationsConcurrentTest() {
      this(2, 10, 2);
   }

   public ConditionalOperationsConcurrentTest(int nodes, int operations, int threads) {
      this.nodes = nodes;
      this.operations = operations;
      this.threads = threads;
   }

   protected final int nodes;
   protected final int operations;
   protected final int threads;
   private static final String SHARED_KEY = "thisIsTheKeyForConcurrentAccess";

   private final String[] validMoves = generateValidMoves();

   private final AtomicBoolean failed = new AtomicBoolean(false);
   private final AtomicBoolean quit = new AtomicBoolean(false);
   private final AtomicInteger liveWorkers = new AtomicInteger();
   private volatile String failureMessage = "";

   protected boolean transactional = false;
   private final CacheMode mode = CacheMode.DIST_SYNC;
   protected LockingMode lockingMode = LockingMode.OPTIMISTIC;
   protected boolean writeSkewCheck = false;

   @BeforeMethod
   public void init() {
      failed.set(false);
      quit.set(false);
      liveWorkers.set(0);
      failureMessage = "";
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder dcc = getDefaultClusteredCacheConfig(mode, transactional);
      dcc.transaction().lockingMode(lockingMode);
      if (writeSkewCheck) {
         dcc.transaction().locking().writeSkewCheck(true);
         dcc.transaction().locking().isolationLevel(IsolationLevel.REPEATABLE_READ);
         dcc.transaction().versioning().enable().scheme(VersioningScheme.SIMPLE);
      }
      createCluster(dcc, nodes);
      waitForClusterToForm();
   }

   public void testReplace() throws Exception {
      List caches = caches(null);
      testOnCaches(caches, new ReplaceOperation(true));
   }

   public void testConditionalRemove() throws Exception {
      List caches = caches(null);
      testOnCaches(caches, new ConditionalRemoveOperation(true));
   }

   public void testPutIfAbsent() throws Exception {
      List caches = caches(null);
      testOnCaches(caches, new PutIfAbsentOperation(true));
   }

   protected void testOnCaches(List<Cache> caches, CacheOperation operation) {
      failed.set(false);
      quit.set(false);
      caches.get(0).put(SHARED_KEY, "initialValue");
      final SharedState state = new SharedState(threads);
      final PostOperationStateCheck stateCheck = new PostOperationStateCheck(caches, state, operation);
      final CyclicBarrier barrier = new CyclicBarrier(threads, stateCheck);
      final String className = getClass().getSimpleName();//in order to be able filter this test's log file correctly
      ExecutorService exec = Executors.newFixedThreadPool(threads, getTestThreadFactory("Mover"));
      for (int threadIndex = 0; threadIndex < threads; threadIndex++) {
         Runnable validMover = new ValidMover(caches, barrier, threadIndex, state, operation);
         exec.execute(validMover);
      }
      exec.shutdown();
      try {
         boolean finished = exec.awaitTermination(5, TimeUnit.MINUTES);
         Assert.assertTrue("Test took too long", finished);
      } catch (InterruptedException e) {
         Assert.fail("Thread interrupted!");
      } finally {
         // Stop the worker threads so that they don't affect the following tests
         exec.shutdownNow();
      }
      Assert.assertFalse(failureMessage, failed.get());
   }

   private String[] generateValidMoves() {
      String[] validMoves = new String[operations];
      for (int i = 0; i < operations; i++) {
         validMoves[i] = "v_" + i;
      }
      print("Valid moves ready");
      return validMoves;
   }

   private void fail(final String message) {
      boolean firstFailure = failed.compareAndSet(false, true);
      if (firstFailure) {
         failureMessage = message;
      }
   }

   private void fail(final Exception e) {
      log.error("Failing because of exception", e);
      fail(e.toString());
   }

   final class ValidMover implements Runnable {

      private final List<Cache> caches;
      private final int threadIndex;
      private final CyclicBarrier barrier;
      private final SharedState state;
      private final CacheOperation operation;

      public ValidMover(List<Cache> caches, CyclicBarrier barrier, int threadIndex, SharedState state, CacheOperation operation) {
         this.caches = caches;
         this.barrier = barrier;
         this.threadIndex = threadIndex;
         this.state = state;
         this.operation = operation;
      }

      @Override
      public void run() {
         int cachePickIndex = threadIndex;
         liveWorkers.incrementAndGet();
         try {
            for (int moveToIndex = threadIndex;
                 moveToIndex < validMoves.length && !barrier.isBroken() && !failed.get() && !quit.get();
                 moveToIndex += threads) {
               operation.beforeOperation(caches.get(0));

               cachePickIndex = ++cachePickIndex % caches.size();
               Cache cache = caches.get(cachePickIndex);
               Object existing = cache.get(SHARED_KEY);
               String targetValue = validMoves[moveToIndex];
               state.beforeOperation(threadIndex, existing, targetValue);
               blockAtTheBarrier();

               boolean successful = operation.execute(cache, SHARED_KEY, existing, targetValue);
               state.afterOperation(threadIndex, existing, targetValue, successful);
               blockAtTheBarrier();
            }
            //not all threads might finish at the same block, so make sure none stays waiting for us when we exit
            quit.set(true);
            barrier.reset();
         } catch (InterruptedException e) {
            log.error("Caught exception", e);
            fail(e);
         } catch (BrokenBarrierException e) {
            log.error("Caught exception", e);
            //just quit
            print("Broken barrier!");
         } catch (RuntimeException e) {
            log.error("Caught exception", e);
            fail(e);
         } finally {
            int andGet = liveWorkers.decrementAndGet();
            barrier.reset();
            print("Thread #" + threadIndex + " terminating. Still " + andGet + " threads alive");
         }
      }

      private void blockAtTheBarrier() throws InterruptedException, BrokenBarrierException {
         try {
            barrier.await(10000, TimeUnit.MILLISECONDS);
         } catch (TimeoutException e) {
            if (!quit.get()) {
               throw new RuntimeException(e);
            }
         }
      }
   }

   static final class SharedState {
      private final SharedThreadState[] threadStates;
      private volatile boolean after = false;

      public SharedState(final int threads) {
         threadStates = new SharedThreadState[threads];
         for (int i = 0; i < threads; i++) {
            threadStates[i] = new SharedThreadState();
         }
      }

      synchronized void beforeOperation(int threadIndex, Object expected, String targetValue) {
         threadStates[threadIndex].beforeReplace(expected, targetValue);
         after = false;
      }

      synchronized void afterOperation(int threadIndex, Object expected, String targetValue, boolean successful) {
         threadStates[threadIndex].afterReplace(expected, targetValue, successful);
         after = true;
      }

      public boolean isAfter() {
         return after;
      }
   }

   static final class SharedThreadState {
      Object beforeExpected;
      Object beforeTargetValue;
      Object afterExpected;
      Object afterTargetValue;
      boolean successfulOperation;

      public void beforeReplace(Object expected, Object targetValue) {
         this.beforeExpected = expected;
         this.beforeTargetValue = targetValue;
      }

      public void afterReplace(Object expected, Object targetValue, boolean replaced) {
         this.afterExpected = expected;
         this.afterTargetValue = targetValue;
         this.successfulOperation = replaced;
      }

      public boolean sameBeforeValue(Object currentStored) {
         return currentStored == null ? beforeExpected == null : currentStored.equals(beforeExpected);
      }
   }

   final class PostOperationStateCheck implements Runnable {

      private final List<Cache> caches;
      private final SharedState state;
      private final CacheOperation operation;
      private volatile int cycle = 0;

      public PostOperationStateCheck(final List<Cache> caches, final SharedState state, CacheOperation operation) {
         this.caches = caches;
         this.state = state;
         this.operation = operation;
      }

      @Override
      public void run() {
         if (state.isAfter()) {
            cycle++;
            log.tracef("Starting cycle %d", cycle);
            if (cycle % (operations / 100) == 0) {
               print((cycle * 100 * threads / operations) + "%");
            }
            checkAfterState();
         } else {
            checkBeforeState();
         }
      }

      private void checkSameValueOnAllCaches() {
         final Object currentStored = caches.get(0).get(SHARED_KEY);
         log.tracef("Value seen by (first) cache %s is %s ", caches.get(0).getAdvancedCache().getRpcManager().getAddress(),
                    currentStored);
         for (Cache c : caches) {
            Object v = c.get(SHARED_KEY);
            Address currentCache = c.getAdvancedCache().getRpcManager().getAddress();
            log.tracef("Value seen by cache %s is %s", currentCache, v);
            boolean sameValue = v == null ? currentStored == null : v.equals(currentStored);
            if (!sameValue) {
               fail("Not all the caches see the same value. first cache: " + currentStored + " cache " + currentCache +" saw " + v);
            }
         }
      }

      private void checkBeforeState() {
         final Object currentStored = caches.get(0).get(SHARED_KEY);
         for (SharedThreadState threadState : state.threadStates) {
            if ( !threadState.sameBeforeValue(currentStored)) {
               fail("Some cache expected a different value than what is stored");
            }
         }
      }

      private void checkAfterState() {
         final Object currentStored = assertTestCorrectness();
         checkSameValueOnAllCaches();
         if (operation.isCas()) {
            checkSingleSuccessfulThread();
            checkSuccessfulOperation(currentStored);
         }
         checkNoLocks();
      }

      private Object assertTestCorrectness() {
         AdvancedCache someCache = caches.get(0).getAdvancedCache();
         final Object currentStored = someCache.get(SHARED_KEY);
         HashSet uniqueValueVerify = new HashSet();
         for (SharedThreadState threadState : state.threadStates) {
            uniqueValueVerify.add(threadState.afterTargetValue);
         }
         if (uniqueValueVerify.size() != threads) {
            fail("test bug");
         }
         return currentStored;
      }

      private void checkNoLocks() {
         for (Cache c : caches) {
            LockManager lockManager = c.getAdvancedCache().getComponentRegistry().getComponent(LockManager.class);
            //locks might be released async, so give it some time
            boolean isLocked = true;
            for (int i = 0; i < 30; i++) {
               if (!lockManager.isLocked(SHARED_KEY)) {
                  isLocked = false;
                  break;
               }
               try {
                  Thread.sleep(500);
               } catch (InterruptedException e) {
                  throw new RuntimeException(e);
               }
            }
            if (isLocked) {
               fail("lock on the entry wasn't cleaned up");
            }
         }
      }

      private void checkSuccessfulOperation(Object currentStored) {
         for (SharedThreadState threadState : state.threadStates) {
            if (threadState.successfulOperation) {
               if (!operation.validateTargetValueForSuccess(threadState.afterTargetValue, currentStored)) {
                  fail("operation successful but the current stored value doesn't match the write operation of the successful thread");
               }
            } else {
               if (threadState.afterTargetValue.equals(currentStored)) {
                  fail("operation not successful (which is fine) but the current stored value matches the write attempt");
               }
            }
         }
      }

      private void checkSingleSuccessfulThread() {
         //for CAS operations there's only one successful thread
         int successfulThreads = 0;
         for (SharedThreadState threadState : state.threadStates) {
            if (threadState.successfulOperation) {
               successfulThreads++;
            }
         }
         if (successfulThreads != 1) {
            fail(successfulThreads + " threads assume a successful replacement! (CAS should succeed on a single thread only)");
         }
      }
   }

   public static abstract class CacheOperation {
      private final boolean isCas;

      protected CacheOperation(boolean cas) {
         isCas = cas;
      }

      public final boolean isCas() {
         return isCas;
      }

      abstract boolean execute(Cache cache, String sharedKey, Object existing, String targetValue);

      abstract void beforeOperation(Cache cache);

      boolean validateTargetValueForSuccess(Object afterTargetValue, Object currentStored) {
         return afterTargetValue.equals(currentStored);
      }
   }

   static class ReplaceOperation extends CacheOperation {

      ReplaceOperation(boolean cas) {
         super(cas);
      }

      @Override
      public boolean execute(Cache cache, String sharedKey, Object existing, String targetValue) {
         try {
            return cache.replace(SHARED_KEY, existing, targetValue);
         } catch (CacheException e) {
            return false;
         }
      }

      @Override
      public void beforeOperation(Cache cache) {
      }
   }

   class PutIfAbsentOperation extends CacheOperation {

      PutIfAbsentOperation(boolean cas) {
         super(cas);
      }

      @Override
      public boolean execute(Cache cache, String sharedKey, Object existing, String targetValue) {
         try {
            Object o = cache.putIfAbsent(SHARED_KEY, targetValue);
            return o == null;
         } catch (CacheException e) {
            return false;
         }
      }

      @Override
      public void beforeOperation(Cache cache) {
         try {
            cache.remove(SHARED_KEY);
         } catch (CacheException e) {
            log.debug("Write skew check error while removing the key", e);
         }
      }
   }

   class ConditionalRemoveOperation extends CacheOperation {

      ConditionalRemoveOperation(boolean cas) {
         super(cas);
      }

      @Override
      public boolean execute(Cache cache, String sharedKey, Object existing, String targetValue) {
         try {
            return cache.remove(SHARED_KEY, existing);
         } catch (CacheException e) {
            return false;
         }
      }

      @Override
      public void beforeOperation(Cache cache) {
         try {
            cache.put(SHARED_KEY, "someValue");
         } catch (CacheException e) {
            log.warn("Write skew check error while inserting the key", e);
         }
      }

      @Override
      boolean validateTargetValueForSuccess(Object afterTargetValue, Object currentStored) {
         return currentStored == null;
      }
   }

   private void print(String s) {
      log.debug(s);
   }
}
