package org.infinispan.spring.embedded.session;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.spring.common.InfinispanTestExecutionListener;
import org.infinispan.spring.common.provider.SpringCache;
import org.infinispan.spring.common.session.AbstractInfinispanSessionRepository;
import org.infinispan.spring.common.session.InfinispanApplicationPublishedBridgeTCK;
import org.infinispan.spring.embedded.provider.BasicConfiguration;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(testName = "spring.embedded.session.EmbeddedApplicationPublishedBridgeTest", groups = "unit")
@ContextConfiguration(classes = BasicConfiguration.class)
@TestExecutionListeners(value = InfinispanTestExecutionListener.class, mergeMode =  TestExecutionListeners.MergeMode.MERGE_WITH_DEFAULTS)
public class EmbeddedApplicationPublishedBridgeTest extends InfinispanApplicationPublishedBridgeTCK {

   private EmbeddedCacheManager embeddedCacheManager;

   @BeforeClass
   public void beforeClass() {
      embeddedCacheManager = TestCacheManagerFactory.createCacheManager(new ConfigurationBuilder());
   }

   @AfterMethod
   public void afterMethod() {
      embeddedCacheManager.getCache().clear();
   }

   @AfterClass
   public void afterClass() {
      embeddedCacheManager.stop();
   }

   @BeforeMethod
   public void beforeMethod() throws Exception {
      super.init();
   }

   @Override
   protected SpringCache createSpringCache() {
      return new SpringCache(embeddedCacheManager.getCache());
   }

   @Override
   protected void callEviction() {
      embeddedCacheManager.getCache().getAdvancedCache().getExpirationManager().processExpiration();
   }

   @Override
   protected AbstractInfinispanSessionRepository createRepository(SpringCache springCache) throws Exception {
      InfinispanEmbeddedSessionRepository sessionRepository = new InfinispanEmbeddedSessionRepository(springCache);
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

   @Override
   public void testEventBridgeWithSessionIdChange() throws Exception {
      super.testEventBridgeWithSessionIdChange();
   }
}
