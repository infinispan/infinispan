package org.infinispan.notifications.cachelistener;

import org.infinispan.Cache;
import org.infinispan.commons.CacheException;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryCreated;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryModified;
import org.infinispan.notifications.cachelistener.event.CacheEntryEvent;
import org.infinispan.remoting.transport.jgroups.SuspectException;
import org.infinispan.test.MultipleCacheManagersTest;
import org.testng.annotations.Test;

import javax.transaction.HeuristicRollbackException;
import javax.transaction.RollbackException;
import java.lang.reflect.Method;

import static org.infinispan.test.TestingUtil.k;
import static org.infinispan.test.TestingUtil.v;
import static org.testng.AssertJUnit.assertTrue;
import static org.testng.AssertJUnit.fail;

/**
 * Tests the behaviour of caches when hooked listeners throw exceptions under
 * different circumstances with XA transactions.
 *
 * @author Galder Zamarreño
 * @author Tomas Sykora
 * @since 5.0
 */
@Test(groups = "functional", testName = "notifications.cachelistener.ListenerExceptionTest")
public class ListenerExceptionTest extends MultipleCacheManagersTest {

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(
            CacheMode.REPL_SYNC, true);
      builder.transaction().useSynchronization(false)
             .recovery().enabled(false);
      createClusteredCaches(2, builder);
   }

   public void testPreOpExceptionListenerOnCreate(Method m) {
      doCallsWithExcepList(m, true, FailureLocation.ON_CREATE);
   }

   public void testPostOpExceptionListenerOnCreate(Method m) {
      doCallsWithExcepList(m, false, FailureLocation.ON_CREATE);
   }

   public void testPreOpExceptionListenerOnPut(Method m) {
      manager(0).getCache().put(k(m), "init");
      doCallsWithExcepList(m, true, FailureLocation.ON_MODIFIED);
   }

   public void testPostOpExceptionListenerOnPut(Method m) {
      manager(0).getCache().put(k(m), "init");
      doCallsWithExcepList(m, false, FailureLocation.ON_MODIFIED);
   }

   public void testPreOpExceptionListenerOnCreateAsync(Method m) {
      doCallsWithExcepListAsync(m, true, FailureLocation.ON_CREATE);
   }

   public void testPostOpExceptionListenerOnCreateAsync(Method m) {
      doCallsWithExcepListAsync(m, false, FailureLocation.ON_CREATE);
   }

   public void testPreOpExceptionListenerOnPutAsync(Method m) {
      manager(0).getCache().put(k(m), "init");
      doCallsWithExcepListAsync(m, true, FailureLocation.ON_MODIFIED);
   }

   public void testPostOpExceptionListenerOnPutAsync(Method m) {
      manager(0).getCache().put(k(m), "init");
      doCallsWithExcepListAsync(m, false, FailureLocation.ON_MODIFIED);
   }

   private void doCallsWithExcepList(Method m, boolean isInjectInPre,
                                     FailureLocation failLoc) {
      Cache<String, String> cache = manager(0).getCache();
      ErrorInducingListener listener =
            new ErrorInducingListener(isInjectInPre, failLoc);
      cache.addListener(listener);
      try {
         cache.put(k(m), v(m));
      } catch (CacheException e) {
         Throwable cause = e.getCause();
         if (isInjectInPre)
            assertExpectedException(cause, cause instanceof SuspectException);
         else
            assertExpectedException(cause, cause instanceof RollbackException || cause instanceof HeuristicRollbackException);

         // Expected, now try to simulate a failover
         listener.injectFailure = false;
         manager(1).getCache().put(k(m), v(m, 2));
         return;
      }
      fail("Should have failed");
   }

   private void assertExpectedException(Throwable cause, boolean condition) {
      assertTrue("Unexpected exception cause " + cause, condition);
   }

   /**
    * If it is used asynchronous listener all callbacks are made in separate thread. Exceptions are only logged, not
    * thrown. See {@link org.infinispan.notifications.impl.AbstractListenerImpl} invoke() method logic
    */
   private void doCallsWithExcepListAsync(Method m, boolean isInjectInPre,
                                          FailureLocation failLoc) {
      Cache<String, String> cache = manager(0).getCache();
      ErrorInducingListenerAsync listenerAsync =
            new ErrorInducingListenerAsync(isInjectInPre, failLoc);
      cache.addListener(listenerAsync);

      cache.put(k(m), v(m));
      assert cache.get(k(m)).equals(v(m));
      assert listenerAsync.caller != Thread.currentThread();
   }

   @Listener
   public static class ErrorInducingListener {
      boolean injectFailure = true;
      boolean isInjectInPre;
      FailureLocation failureLocation;

      public ErrorInducingListener(boolean injectInPre, FailureLocation failLoc) {
         this.isInjectInPre = injectInPre;
         this.failureLocation = failLoc;
      }

      @CacheEntryCreated
      @SuppressWarnings("unused")
      public void entryCreated(CacheEntryEvent event) throws Exception {
         if (failureLocation == FailureLocation.ON_CREATE)
            injectFailure(event);
      }

      @CacheEntryModified
      @SuppressWarnings("unused")
      public void entryModified(CacheEntryEvent event) throws Exception {
         if (failureLocation == FailureLocation.ON_MODIFIED)
            injectFailure(event);
      }

      private void injectFailure(CacheEntryEvent event) {
         if (injectFailure) {
            if (isInjectInPre && event.isPre())
               throwSuspectException();
            else if (!isInjectInPre && !event.isPre())
               throwSuspectException();
         }
      }

      private void throwSuspectException() {
         throw new SuspectException(String.format(
            "Simulated suspicion when isPre=%b and in %s",
            isInjectInPre, failureLocation));
      }

   }

   @Listener(sync = false)
   public static class ErrorInducingListenerAsync {
      boolean injectFailure = true;
      boolean isInjectInPre;
      FailureLocation failureLocation;
      Thread caller;

      public ErrorInducingListenerAsync(boolean injectInPre, FailureLocation failLoc) {
         this.isInjectInPre = injectInPre;
         this.failureLocation = failLoc;
      }

      @CacheEntryCreated
      @SuppressWarnings("unused")
      public void entryCreated(CacheEntryEvent event) throws Exception {
         caller = Thread.currentThread();
         if (failureLocation == FailureLocation.ON_CREATE)
            injectFailure(event);
      }

      @CacheEntryModified
      @SuppressWarnings("unused")
      public void entryModified(CacheEntryEvent event) throws Exception {
         caller = Thread.currentThread();
         if (failureLocation == FailureLocation.ON_MODIFIED)
            injectFailure(event);
      }

      private void injectFailure(CacheEntryEvent event) {
         if (injectFailure) {
            if (isInjectInPre && event.isPre())
               throwSuspectException();
            else if (!isInjectInPre && !event.isPre())
               throwSuspectException();
         }
      }

      private void throwSuspectException() {
         throw new SuspectException(String.format(
               "Simulated ASYNC suspicion when isPre=%b and in %s",
               isInjectInPre, failureLocation));
      }

   }

   private static enum FailureLocation {
      ON_CREATE, ON_MODIFIED
   }
}