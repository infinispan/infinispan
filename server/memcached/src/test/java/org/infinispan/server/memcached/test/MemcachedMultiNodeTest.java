package org.infinispan.server.memcached.test;

import static org.infinispan.server.memcached.test.MemcachedTestingUtil.createMemcachedClient;
import static org.infinispan.server.memcached.test.MemcachedTestingUtil.serverBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.infinispan.Cache;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.core.test.ServerTestingUtil;
import org.infinispan.server.memcached.MemcachedServer;
import org.infinispan.server.memcached.configuration.MemcachedProtocol;
import org.infinispan.server.memcached.configuration.MemcachedServerConfigurationBuilder;
import org.infinispan.test.MultipleCacheManagersTest;
import org.testng.annotations.AfterClass;

import net.spy.memcached.MemcachedClient;

/**
 * @author Galder Zamarreño
 */
public abstract class MemcachedMultiNodeTest extends MultipleCacheManagersTest {

   protected static String cacheName = "MemcachedReplSync";
   protected int nodeCount = 2;

   protected List<MemcachedServer> servers = new ArrayList<>(nodeCount);
   protected List<MemcachedClient> clients = new ArrayList<>(nodeCount);
   protected Map<Cache<String, byte[]>, MemcachedClient> cacheClient = new HashMap<>();

   protected int timeout = 60;

   @Override
   protected void createCacheManagers() {
      for (int i = 0; i < nodeCount; ++i) {
         cacheManagers.add(createCacheManager(i));
      }

      waitForClusterToForm();
      MemcachedServerConfigurationBuilder builder = serverBuilder().defaultCacheName(cacheName).protocol(getProtocol());
      MemcachedServer server1 = new MemcachedServer();
      server1.start(builder.build(), cacheManagers.get(0));
      servers.add(server1);
      MemcachedServer server2 = new MemcachedServer();
      server2.start(builder.port(server1.getPort() + 50).build(), cacheManagers.get(1));
      servers.add(server2);
      servers.forEach(s -> {
         MemcachedClient client;
         try {
            client = createMemcachedClient(s);
         } catch (IOException e) {
            throw new AssertionError(e);
         }
         clients.add(client);
         Cache<String, byte[]> cache = s.getCacheManager().getCache(cacheName);
         cacheClient.put(cache, client);
      });
   }

   protected abstract EmbeddedCacheManager createCacheManager(int index);

   protected MemcachedProtocol getProtocol() {
      return MemcachedProtocol.TEXT;
   }

   @AfterClass(alwaysRun = true)
   @Override
   public void destroy() {
      super.destroy();
      log.debug("Test finished, close Hot Rod server");
      clients.forEach(MemcachedTestingUtil::killMemcachedClient);
      servers.forEach(ServerTestingUtil::killServer);
   }
}
