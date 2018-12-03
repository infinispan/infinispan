package org.infinispan.spring.remote.session;

import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.server.hotrod.test.HotRodTestingUtil;
import org.infinispan.spring.common.provider.SpringCache;
import org.infinispan.spring.common.session.AbstractInfinispanSessionRepository;
import org.infinispan.spring.common.session.InfinispanSessionRepositoryTCK;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(testName = "spring.session.InfinispanRemoteSessionRepositoryTest", groups = "functional")
public class InfinispanRemoteSessionRepositoryTest extends InfinispanSessionRepositoryTCK {

   private EmbeddedCacheManager embeddedCacheManager;
   private HotRodServer hotrodServer;
   private RemoteCacheManager remoteCacheManager;
   private ThreadPoolTaskExecutor executor;

   @BeforeClass
   public void beforeClass() {
      org.infinispan.configuration.cache.ConfigurationBuilder cacheConfiguration = new org.infinispan.configuration.cache.ConfigurationBuilder();
      embeddedCacheManager = TestCacheManagerFactory.createCacheManager(cacheConfiguration);
      hotrodServer = HotRodTestingUtil.startHotRodServer(embeddedCacheManager, 19723);
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.addServer().host("localhost").port(hotrodServer.getPort());
      remoteCacheManager = new RemoteCacheManager(builder.build());
      executor = new ThreadPoolTaskExecutor();
      executor.setCorePoolSize(4);
      executor.setMaxPoolSize(4);
      executor.setThreadNamePrefix("default_task_executor_thread");
      executor.initialize();
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
   protected AbstractInfinispanSessionRepository createRepository(SpringCache springCache) {
      InfinispanRemoteSessionRepository sessionRepository = new InfinispanRemoteSessionRepository(springCache, executor);
      sessionRepository.afterPropertiesSet();
      return sessionRepository;
   }

   @Test(expectedExceptions = NullPointerException.class)
   @Override
   public void testThrowingExceptionOnNullSpringCache() throws Exception {
      super.testThrowingExceptionOnNullSpringCache();
   }

   @Override
   public void testCreatingSession() throws Exception {
      super.testCreatingSession();
   }

   @Override
   public void testSavingSession() throws Exception {
      super.testSavingSession();
   }

   @Override
   public void testDeletingSession() throws Exception {
      super.testDeletingSession();
   }

   @Override
   public void testEvictingSession() throws Exception {
      super.testEvictingSession();
   }

   @Override
   public void testUpdatingTTLOnAccessingData() throws Exception {
      super.testUpdatingTTLOnAccessingData();
   }

}
