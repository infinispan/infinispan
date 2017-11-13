package org.infinispan.client.hotrod.retry;

import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.getLoadBalancer;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.marshall;

import java.net.SocketAddress;

import org.infinispan.AdvancedCache;
import org.infinispan.client.hotrod.HitsAwareCacheManagersTest;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.impl.RemoteCacheImpl;
import org.infinispan.client.hotrod.impl.consistenthash.ConsistentHash;
import org.infinispan.client.hotrod.impl.transport.netty.ChannelFactory;
import org.infinispan.client.hotrod.impl.transport.tcp.RoundRobinBalancingStrategy;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.client.hotrod.test.InternalRemoteCacheManager;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterMethod;

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
   protected ChannelFactory channelFactory;
   protected ConfigurationBuilder config;
   protected RoundRobinBalancingStrategy strategy;

   public AbstractRetryTest() {
      cleanup = CleanupPhase.AFTER_METHOD;
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

      hotRodServer1 = createStartHotRodServer(manager(0));
      addr2hrServer.put(getAddress(hotRodServer1), hotRodServer1);
      hotRodServer2 = createStartHotRodServer(manager(1));
      addr2hrServer.put(getAddress(hotRodServer2), hotRodServer2);
      hotRodServer3 = createStartHotRodServer(manager(2));
      addr2hrServer.put(getAddress(hotRodServer3), hotRodServer3);

      waitForClusterToForm();

      remoteCacheManager = createRemoteCacheManager(hotRodServer1.getPort());
      remoteCache = (RemoteCacheImpl) remoteCacheManager.getCache();
      channelFactory = ((InternalRemoteCacheManager) remoteCacheManager).getChannelFactory();
      strategy = getLoadBalancer(remoteCacheManager);
      addInterceptors();

      assert super.cacheManagers.size() == 3;
   }

   protected RemoteCacheManager createRemoteCacheManager(int port) {
      org.infinispan.client.hotrod.configuration.ConfigurationBuilder builder =
         new org.infinispan.client.hotrod.configuration.ConfigurationBuilder();
      builder
         .forceReturnValues(true)
         .connectionTimeout(5)
         .connectionPool().maxActive(1) //this ensures that only one server is active at a time
         .addServer().host("127.0.0.1").port(port);
      return new InternalRemoteCacheManager(builder.build());
   }

   protected HotRodServer createStartHotRodServer(EmbeddedCacheManager manager) {
      return HotRodClientTestingUtil.startHotRodServer(manager);
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

   protected AdvancedCache<?, ?> cacheToHit(Object key) {
      ConsistentHash consistentHash = channelFactory.getConsistentHash(RemoteCacheManager.cacheNameBytes());
      SocketAddress expectedServer = consistentHash.getServer(marshall(key));
      return addr2hrServer.get(expectedServer).getCacheManager().getCache().getAdvancedCache();
   }
}
