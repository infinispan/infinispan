package org.infinispan.server.memcached;

import net.spy.memcached.MemcachedClient;
import org.infinispan.Cache;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.core.test.ServerTestingUtil;
import static org.infinispan.server.memcached.test.MemcachedTestingUtil.*;

import org.infinispan.server.memcached.test.MemcachedTestingUtil;
import org.infinispan.test.MultipleCacheManagersTest;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * // TODO: Document this
 * @author Galder Zamarreño
 * @since // TODO
 */
abstract class MemcachedMultiNodeTest extends MultipleCacheManagersTest {

   protected static String cacheName = "MemcachedReplSync";
   protected int nodeCount = 2;

   protected List<MemcachedServer> servers = new ArrayList<>(nodeCount);
   protected List<MemcachedClient> clients = new ArrayList<>(nodeCount);
   protected Map<Cache<String, byte[]>, MemcachedClient> cacheClient = new HashMap<>();

   protected int timeout = 60;

   @Override
   protected void createCacheManagers() throws Throwable {
      for (int i = 0; i < nodeCount; ++i) {
         cacheManagers.add(createCacheManager(i));
      }

      waitForClusterToForm();

      servers.add(startMemcachedTextServer(cacheManagers.get(0), cacheName));

      servers.add(startMemcachedTextServer(cacheManagers.get(1), servers.get(0).getPort() + 50, cacheName));
      servers.forEach(s -> {
         MemcachedClient client = null;
         try {
            client = createMemcachedClient(60000, s.getPort());
         } catch (IOException e) {
            throw new AssertionError(e);
         }
         clients.add(client);
         Cache<String, byte[]> cache = s.getCacheManager().getCache(cacheName);
         cacheClient.put(cache, client);
      });
   }

   protected abstract EmbeddedCacheManager createCacheManager(int index);

   @AfterClass(alwaysRun = true)
   @Override
   public void destroy() {
      super.destroy();
      log.debug("Test finished, close Hot Rod server");
      clients.forEach(MemcachedTestingUtil::killMemcachedClient);
      servers.forEach(ServerTestingUtil::killServer);
   }

}
