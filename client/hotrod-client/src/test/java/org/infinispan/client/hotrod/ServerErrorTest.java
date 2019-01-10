package org.infinispan.client.hotrod;

import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.killRemoteCacheManager;
import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.killServers;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.unmarshall;
import static org.infinispan.test.TestingUtil.k;
import static org.infinispan.test.TestingUtil.v;

import java.lang.reflect.Method;
import java.util.Properties;

import org.infinispan.client.hotrod.exceptions.HotRodClientException;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryCreated;
import org.infinispan.notifications.cachelistener.event.CacheEntryEvent;
import org.infinispan.remoting.transport.jgroups.SuspectException;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

/**
 * Tests HotRod client and server behaivour when server throws a server error
 *
 * @author Galder Zamarreño
 * @since 4.2
 */
@Test(groups = "functional", testName = "client.hotrod.ServerErrorTest")
public class ServerErrorTest extends SingleCacheManagerTest {

   private HotRodServer hotrodServer;
   private RemoteCacheManager remoteCacheManager;
   private RemoteCache<String, String> remoteCache;

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      cacheManager = TestCacheManagerFactory
            .createCacheManager(hotRodCacheConfiguration());
      hotrodServer = HotRodClientTestingUtil.startHotRodServer(cacheManager);

      remoteCacheManager = getRemoteCacheManager();
      remoteCache = remoteCacheManager.getCache();

      return cacheManager;
   }

   protected RemoteCacheManager getRemoteCacheManager() {
      Properties config = new Properties();
      org.infinispan.client.hotrod.configuration.ConfigurationBuilder clientBuilder =
            HotRodClientTestingUtil.newRemoteConfigurationBuilder();
      clientBuilder.addServer().host("localhost").port(hotrodServer.getPort());
      return new RemoteCacheManager(clientBuilder.build());
   }

   @AfterClass
   public void shutDownHotrod() {
      killRemoteCacheManager(remoteCacheManager);
      killServers(hotrodServer);
   }

   public void testErrorWhileDoingPut(Method m) throws Exception {
      cache.addListener(new ErrorInducingListener());
      remoteCache = remoteCacheManager.getCache();

      remoteCache.put(k(m), v(m));
      assert remoteCache.get(k(m)).equals(v(m));

      try {
         remoteCache.put("FailFailFail", "whatever...");
      } catch (HotRodClientException e) {
         // ignore
      }

      try {
         remoteCache.put(k(m, 2), v(m, 2));
         assert remoteCache.get(k(m, 2)).equals(v(m, 2));
      } catch (Exception e) {
         log.error("Error sending request after server failure", e);
         throw e;
      }

   }

   @Listener
   public static class ErrorInducingListener {
      @CacheEntryCreated
      @SuppressWarnings("unused")
      public void entryCreated(CacheEntryEvent<byte[], byte[]> event) throws Exception {
         if (event.isPre() && unmarshall(event.getKey()) == "FailFailFail") {
            throw new SuspectException("Simulated suspicion");
         }
      }
   }
}
