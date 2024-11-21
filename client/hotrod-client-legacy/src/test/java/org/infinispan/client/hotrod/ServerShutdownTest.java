package org.infinispan.client.hotrod;

import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.killServers;
import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.withRemoteCacheManager;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.infinispan.test.TestingUtil.withCacheManager;
import static org.testng.AssertJUnit.assertEquals;

import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.client.hotrod.test.RemoteCacheManagerCallable;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.CacheManagerCallable;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

/**
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
@Test(testName = "client.hotrod.ServerShutdownTest", groups = "functional")
public class ServerShutdownTest extends AbstractInfinispanTest {

   public void testServerShutdownWithConnectedClient() {
      withCacheManager(new CacheManagerCallable(
            TestCacheManagerFactory.createCacheManager(
                  hotRodCacheConfiguration())) {
         @Override
         public void call() {
            HotRodServer hotrodServer = HotRodClientTestingUtil.startHotRodServer(cm);
            try {
               org.infinispan.client.hotrod.configuration.ConfigurationBuilder clientBuilder =
                     HotRodClientTestingUtil.newRemoteConfigurationBuilder();
               clientBuilder.addServer().host("localhost").port(hotrodServer.getPort());
               withRemoteCacheManager(new RemoteCacheManagerCallable(
                     new RemoteCacheManager(clientBuilder.build())) {
                  @Override
                  public void call() {
                     RemoteCache remoteCache = rcm.getCache();
                     remoteCache.put("k","v");
                     assertEquals("v", remoteCache.get("k"));
                  }
               });
            } finally {
               killServers(hotrodServer);
            }
         }
      });
   }

   public void testServerShutdownWithoutConnectedClient() {
      withCacheManager(new CacheManagerCallable(
            TestCacheManagerFactory.createCacheManager(
                  hotRodCacheConfiguration())) {
         @Override
         public void call() {
            HotRodServer hotrodServer = HotRodClientTestingUtil.startHotRodServer(cm);
            try {
               org.infinispan.client.hotrod.configuration.ConfigurationBuilder clientBuilder =
                     HotRodClientTestingUtil.newRemoteConfigurationBuilder();
               clientBuilder.addServer().host("localhost").port(hotrodServer.getPort());
               withRemoteCacheManager(new RemoteCacheManagerCallable(
                     new RemoteCacheManager(clientBuilder.build())) {
                  @Override
                  public void call() {
                     RemoteCache remoteCache = rcm.getCache();
                     remoteCache.put("k","v");
                     assertEquals("v", remoteCache.get("k"));
                  }
               });
            } finally {
               killServers(hotrodServer);
            }
         }
      });
   }

}
