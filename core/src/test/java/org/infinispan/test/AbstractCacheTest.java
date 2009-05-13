package org.infinispan.test;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.config.Configuration;
import org.infinispan.container.DataContainer;
import org.infinispan.context.InvocationContext;
import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.loaders.CacheLoaderManager;
import org.infinispan.manager.CacheManager;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.remoting.ReplicationQueue;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import javax.transaction.TransactionManager;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

/**
 * Base class for {@link org.infinispan.test.SingleCacheManagerTest} and {@link org.infinispan.test.MultipleCacheManagersTest}.
 *
 * @author Mircea.Markus@jboss.com
 */
public class AbstractCacheTest {

   protected final Log log = LogFactory.getLog(getClass());

   public static enum CleanupPhase {
      AFTER_METHOD, AFTER_TEST
   }

   protected CleanupPhase cleanup = CleanupPhase.AFTER_TEST;

   public void clearContent(CacheManager cacheManager) {
      if (cacheManager != null) {
         Set<Cache> runningCaches = getRunningCaches(cacheManager);
         for (Cache cache : runningCaches) {
            clearRunningTx(cache);
         }
         if (!cacheManager.getStatus().allowInvocations()) return;
         for (Cache cache : runningCaches) {
            removeInMemoryData(cache);
            clearCacheLoader(cache);
            clearReplicationQueues(cache);
            InvocationContext invocationContext = ((AdvancedCache) cache).getInvocationContextContainer().createInvocationContext();
         }
      }
   }

   private void clearReplicationQueues(Cache cache) {
      ReplicationQueue queue = TestingUtil.extractComponent(cache, ReplicationQueue.class);
      if (queue != null) queue.reset();
   }

   @SuppressWarnings(value = "unchecked")
   protected Set<Cache> getRunningCaches(CacheManager cacheManager) {
      ConcurrentMap<String, Cache> caches = (ConcurrentMap<String, Cache>) TestingUtil.extractField(DefaultCacheManager.class, cacheManager, "caches");
      if (caches == null) return Collections.emptySet();
      Set<Cache> result = new HashSet<Cache>();
      for (Cache cache : caches.values()) {
         if (cache.getStatus() == ComponentStatus.RUNNING) result.add(cache);
      }
      return result;
   }

   private void clearCacheLoader(Cache cache) {
      CacheLoaderManager cacheLoaderManager = TestingUtil.extractComponent(cache, CacheLoaderManager.class);
      if (cacheLoaderManager != null && cacheLoaderManager.getCacheStore() != null) {
         try {
            cacheLoaderManager.getCacheStore().clear();
         } catch (Exception e) {
            throw new RuntimeException(e);
         }
      }
   }

   private void removeInMemoryData(Cache cache) {
      CacheManager mgr = cache.getCacheManager();
      Address a = mgr.getAddress();
      String str;
      if (a == null)
         str = "a non-clustered cache manager";
      else
         str = "a cache manager at address " + a;
      log.debug("Cleaning data for cache '{0}' on {1}", cache.getName(), str);
      DataContainer dataContainer = TestingUtil.extractComponent(cache, DataContainer.class);
      log.debug("removeInMemoryData(): dataContainerBefore == {0}", dataContainer);
      dataContainer.clear();
      log.debug("removeInMemoryData(): dataContainerAfter == {0}", dataContainer);
   }

   private void clearRunningTx(Cache cache) {
      if (cache != null) {
         TransactionManager txm = TestingUtil.getTransactionManager(cache);
         if (txm == null) return;
         try {
            txm.rollback();
         }
         catch (Exception e) {
            // don't care
         }
      }
   }

   /**
    * When multiple test merhods operate on same cluster, sync commit and rollback are mandatory. This is in order to
    * make sure that an commit message will be dispatched in the same test method it was triggered and it will not
    * interfere with further log messages.
    */
   protected Configuration getDefaultClusteredConfig(Configuration.CacheMode mode) {
      Configuration configuration = new Configuration();
      configuration.setCacheMode(mode);
      configuration.setSyncCommitPhase(true);
      configuration.setSyncRollbackPhase(true);
      configuration.setFetchInMemoryState(false);
      return configuration;
   }
}
