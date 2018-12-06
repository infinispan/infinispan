package org.infinispan.spring.remote.session;

import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.server.hotrod.test.HotRodTestingUtil;
import org.infinispan.spring.common.provider.SpringCache;
import org.infinispan.spring.common.session.AbstractInfinispanSessionRepository;
import org.infinispan.spring.common.session.InfinispanApplicationPublishedBridgeTCK;
import org.infinispan.spring.remote.session.configuration.EnableInfinispanRemoteHttpSession;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
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
   private ThreadPoolTaskExecutor executor;

   @BeforeClass
   public void beforeClass() {
      embeddedCacheManager = TestCacheManagerFactory.createCacheManager();
      hotrodServer = HotRodTestingUtil.startHotRodServer(embeddedCacheManager, 19723);
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.addServer().host("localhost").port(hotrodServer.getPort());
      remoteCacheManager = new RemoteCacheManager(builder.build());
      executor = new ThreadPoolTaskExecutor();
      executor.setCorePoolSize(EnableInfinispanRemoteHttpSession.DEFAULT_EXECUTOR_POOL_SIZE);
      executor.setMaxPoolSize(EnableInfinispanRemoteHttpSession.DEFAULT_EXECUTOR_MAX_POOL_SIZE);
      executor.setThreadNamePrefix(EnableInfinispanRemoteHttpSession.DEFAULT_EXECUTOR_THREAD_NAME_PREFIX);
      executor.initialize();
   }

   @AfterMethod
   public void afterMethod() {
      remoteCacheManager.getCache().clear();
   }

   @AfterClass
   public void afterClass() {
      embeddedCacheManager.stop();
      remoteCacheManager.stop();
      hotrodServer.stop();
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
   protected AbstractInfinispanSessionRepository createRepository(SpringCache springCache) {
      InfinispanRemoteSessionRepository sessionRepository = new InfinispanRemoteSessionRepository(springCache, executor);
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
