package org.infinispan.notifications.cachelistener;

import static org.infinispan.test.TestingUtil.k;
import static org.infinispan.test.TestingUtil.v;
import static org.testng.AssertJUnit.assertNotNull;

import java.lang.reflect.Method;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryCreated;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryModified;
import org.infinispan.notifications.cachelistener.event.CacheEntryEvent;
import org.infinispan.test.MultipleCacheManagersTest;
import org.testng.annotations.Test;

/**
 * Verifies that a sync listener exception on a backup node does not permanently
 * block subsequent operations on the same key due to a leaked DataOperationOrderer future.
 *
 * @since 16.2
 * @see <a href="https://github.com/infinispan/infinispan/issues/17529">#17529</a>
 */
@Test(groups = "functional", testName = "notifications.cachelistener.ListenerExceptionReplicationTest")
public class ListenerExceptionReplicationTest extends MultipleCacheManagersTest {

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, false);
      builder.memory().maxCount(100);
      createClusteredCaches(2, builder);
   }

   public void testCreateListenerFailureOnBackup(Method m) throws Exception {
      doTestListenerFailureOnBackup(m, false);
   }

   public void testModifyListenerFailureOnBackup(Method m) throws Exception {
      doTestListenerFailureOnBackup(m, true);
   }

   private void doTestListenerFailureOnBackup(Method m, boolean preExisting) throws Exception {
      Cache<String, String> primary = cache(0);
      Cache<String, String> backup = cache(1);

      if (preExisting) {
         primary.put(k(m), "init");
      }

      BackupFailingListener listener = new BackupFailingListener();
      backup.addListener(listener);
      try {
         primary.put(k(m), v(m));
      } catch (Exception expected) {
      } finally {
         backup.removeListener(listener);
      }

      // Without the commitEntryOrdered fix, this hangs because the
      // DataOperationOrderer future was never completed for this key
      Future<String> future = fork(() -> primary.put(k(m), v(m, 2)));
      try {
         future.get(10, TimeUnit.SECONDS);
      } catch (TimeoutException e) {
         throw new AssertionError("Subsequent put on same key hung - DataOperationOrderer future was leaked", e);
      }

      assertNotNull(primary.get(k(m)));
      assertNotNull(backup.get(k(m)));
   }

   @Listener
   public static class BackupFailingListener {
      @CacheEntryCreated
      @SuppressWarnings("unused")
      public void entryCreated(CacheEntryEvent event) {
         if (!event.isPre()) {
            throw new RuntimeException("Simulated listener failure on backup");
         }
      }

      @CacheEntryModified
      @SuppressWarnings("unused")
      public void entryModified(CacheEntryEvent event) {
         if (!event.isPre()) {
            throw new RuntimeException("Simulated listener failure on backup");
         }
      }
   }
}
