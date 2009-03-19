package org.horizon.test;

import org.horizon.AdvancedCache;
import org.horizon.Cache;
import org.horizon.container.DataContainer;
import org.horizon.context.InvocationContext;
import org.horizon.lifecycle.ComponentStatus;
import org.horizon.loader.CacheLoaderManager;
import org.horizon.logging.Log;
import org.horizon.logging.LogFactory;
import org.horizon.manager.CacheManager;
import org.horizon.manager.DefaultCacheManager;
import org.horizon.remoting.ReplicationQueue;
import org.horizon.remoting.transport.Address;

import javax.transaction.TransactionManager;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

/**
 * Base class for {@link org.horizon.test.SingleCacheManagerTest} and {@link org.horizon.test.MultipleCacheManagersTest}.
 *
 * @author Mircea.Markus@jboss.com
 */
public class AbstractCacheTest {

   protected final Log log = LogFactory.getLog(getClass());

   public void clearContent(CacheManager cacheManager) {
      Set<Cache> runningCaches = getRunningCaches(cacheManager);
      for (Cache cache : runningCaches) {
         clearRunningTx(cache);
      }
      if (!cacheManager.getStatus().allowInvocations()) return;
      for (Cache cache : runningCaches) {
         removeInMemoryData(cache);
         clearCacheLoader(cache);
         clearReplicationQueues(cache);
         InvocationContext invocationContext = ((AdvancedCache) cache).getInvocationContextContainer().get();
         if (invocationContext != null) invocationContext.reset();
      }
   }

   private void clearReplicationQueues(Cache cache) {
      ReplicationQueue queue = TestingUtil.extractComponent(cache, ReplicationQueue.class);
      if (queue != null) queue.reset();
   }

   @SuppressWarnings(value = "unchecked")
   protected Set<Cache> getRunningCaches(CacheManager cacheManager) {
      ConcurrentMap<String, Cache> caches = (ConcurrentMap<String, Cache>) TestingUtil.extractField(DefaultCacheManager.class, cacheManager, "caches");
      Set<Cache> result = new HashSet<Cache>();
      for (Cache cache : caches.values()) {
         if (cache.getStatus() == ComponentStatus.RUNNING)
            result.add(cache);
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
}
