package org.infinispan.spring.session;

import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.server.hotrod.test.HotRodTestingUtil;
import org.infinispan.spring.provider.SpringCache;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(testName = "spring.session.RemoteApplicationPublishedBridgeTest", groups = "unit")
public class RemoteApplicationPublishedBridgeTest extends InfinispanApplicationPublishedBridgeTCK {

   private EmbeddedCacheManager embeddedCacheManager;
   private HotRodServer hotrodServer;
   private RemoteCacheManager remoteCacheManager;

   @BeforeClass
   public void beforeClass() {
      embeddedCacheManager = TestCacheManagerFactory.createCacheManager();
      hotrodServer = HotRodTestingUtil.startHotRodServer(embeddedCacheManager, 19723);
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.addServer().host("localhost").port(hotrodServer.getPort());
      remoteCacheManager = new RemoteCacheManager(builder.build());
   }

   @AfterMethod
   public void afterMethod() {
      remoteCacheManager.getCache().clear();
   }

   @AfterClass
   public void afterClass() {
      remoteCacheManager.stop();
      hotrodServer.stop();
      embeddedCacheManager.stop();
   }

   @BeforeMethod
   public void beforeMethod() throws Exception {
      super.init();
   }

   @Override
   protected SpringCache createSpringCache() {
      return new SpringCache(remoteCacheManager.getCache());
   }

   @Override
   protected void callEviction() {
      embeddedCacheManager.getCache().getAdvancedCache().getExpirationManager().processExpiration();
   }

   @Override
   protected AbstractInfinispanSessionRepository createRepository(SpringCache springCache) throws Exception {
      InfinispanRemoteSessionRepository sessionRepository = new InfinispanRemoteSessionRepository(springCache);
      sessionRepository.afterPropertiesSet();
      return sessionRepository;
   }

   @Override
   public void testEventBridge() throws Exception {
      super.testEventBridge();
   }

   @Override
   public void testUnregistration() throws Exception {
      super.testUnregistration();
   }
}
