package org.infinispan.client.hotrod.retry;

import org.infinispan.AdvancedCache;
import org.infinispan.client.hotrod.HitsAwareCacheManagersTest;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.test.InternalRemoteCacheManager;
import org.infinispan.client.hotrod.impl.RemoteCacheImpl;
import org.infinispan.client.hotrod.impl.transport.tcp.RoundRobinBalancingStrategy;
import org.infinispan.client.hotrod.impl.transport.tcp.TcpTransportFactory;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterMethod;

import java.net.SocketAddress;
import java.util.Properties;

import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.getLoadBalancer;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;

/**
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
public abstract class AbstractRetryTest extends HitsAwareCacheManagersTest {
   
   protected HotRodServer hotRodServer1;
   protected HotRodServer hotRodServer2;
   protected HotRodServer hotRodServer3;

   RemoteCacheImpl<Object, Object> remoteCache;
   protected RemoteCacheManager remoteCacheManager;
   protected TcpTransportFactory tcpTransportFactory;
   protected ConfigurationBuilder config;
   protected RoundRobinBalancingStrategy strategy;

   public AbstractRetryTest() {
      cleanup = CleanupPhase.AFTER_METHOD;
   }

   @Override
   protected void assertSupportedConfig() {
      // no-op
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      config = hotRodCacheConfiguration(getCacheConfig());
      EmbeddedCacheManager cm1 = TestCacheManagerFactory.createClusteredCacheManager(config);
      EmbeddedCacheManager cm2 = TestCacheManagerFactory.createClusteredCacheManager(config);
      EmbeddedCacheManager cm3 = TestCacheManagerFactory.createClusteredCacheManager(config);
      registerCacheManager(cm1);
      registerCacheManager(cm2);
      registerCacheManager(cm3);

      hotRodServer1 = HotRodClientTestingUtil.startHotRodServer(manager(0));
      addr2hrServer.put(getAddress(hotRodServer1), hotRodServer1);
      hotRodServer2 = HotRodClientTestingUtil.startHotRodServer(manager(1));
      addr2hrServer.put(getAddress(hotRodServer2), hotRodServer2);
      hotRodServer3 = HotRodClientTestingUtil.startHotRodServer(manager(2));
      addr2hrServer.put(getAddress(hotRodServer3), hotRodServer3);

      waitForClusterToForm();

      Properties clientConfig = new Properties();
      clientConfig.put("infinispan.client.hotrod.server_list", "localhost:" + hotRodServer1.getPort());
      clientConfig.put("infinispan.client.hotrod.force_return_values", "true");
      clientConfig.put("infinispan.client.hotrod.connect_timeout", "5");
      clientConfig.put("maxActive",1); //this ensures that only one server is active at a time

      remoteCacheManager = new InternalRemoteCacheManager(clientConfig);
      remoteCache = (RemoteCacheImpl) remoteCacheManager.getCache();
      tcpTransportFactory = (TcpTransportFactory) ((InternalRemoteCacheManager) remoteCacheManager).getTransportFactory();
      strategy = getLoadBalancer(remoteCacheManager);
      addInterceptors();

      assert super.cacheManagers.size() == 3;
   }

   @AfterMethod(alwaysRun = true)
   @Override
   protected void clearContent() throws Throwable {
      if (cleanupAfterMethod()) {
         HotRodClientTestingUtil.killRemoteCacheManagers(remoteCacheManager);
         HotRodClientTestingUtil.killServers(hotRodServer1, hotRodServer2, hotRodServer3);
      }
      super.clearContent();
   }

   protected abstract ConfigurationBuilder getCacheConfig();

   protected AdvancedCache<?, ?> nextCacheToHit() {
      SocketAddress expectedServer = strategy.getServers()[strategy.getNextPosition()];
      return addr2hrServer.get(expectedServer).getCacheManager().getCache().getAdvancedCache();
   }

}
