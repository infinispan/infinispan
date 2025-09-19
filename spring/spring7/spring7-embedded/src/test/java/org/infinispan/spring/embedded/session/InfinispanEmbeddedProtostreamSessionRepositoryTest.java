package org.infinispan.spring.embedded.session;

import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.spring.common.InfinispanTestExecutionListener;
import org.infinispan.spring.common.provider.SpringCache;
import org.infinispan.spring.common.session.AbstractInfinispanSessionRepository;
import org.infinispan.spring.common.session.InfinispanSessionRepositoryTCK;
import org.infinispan.spring.embedded.provider.BasicConfiguration;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Factory;
import org.testng.annotations.Test;

@Test(testName = "spring.embedded.session.InfinispanEmbeddedProtostreamSessionRepositoryTest", groups = "unit")
@ContextConfiguration(classes = BasicConfiguration.class)
@TestExecutionListeners(value = InfinispanTestExecutionListener.class, mergeMode =  TestExecutionListeners.MergeMode.MERGE_WITH_DEFAULTS)
public class InfinispanEmbeddedProtostreamSessionRepositoryTest extends InfinispanSessionRepositoryTCK {

   private EmbeddedCacheManager embeddedCacheManager;
   private EmbeddedCacheManager cacheManager2;
   private EmbeddedCacheManager cacheManager3;

   @Factory
   public Object[] factory() {
      return new Object[]{
            new InfinispanEmbeddedProtostreamSessionRepositoryTest().mediaType(MediaType.APPLICATION_PROTOSTREAM),
            };
   }

   @BeforeClass
   public void beforeClass() {
      ConfigurationBuilder defaultCacheBuilder = new ConfigurationBuilder();
      defaultCacheBuilder.clustering().cacheMode(CacheMode.DIST_SYNC).encoding().mediaType(mediaType.getTypeSubtype());
      embeddedCacheManager = TestCacheManagerFactory.createClusteredCacheManager(defaultCacheBuilder);
      cacheManager2 = TestCacheManagerFactory.createClusteredCacheManager(defaultCacheBuilder);
      cacheManager3 = TestCacheManagerFactory.createClusteredCacheManager(defaultCacheBuilder);
   }

   @AfterMethod
   public void afterMethod() {
      embeddedCacheManager.getCache().clear();
   }

   @AfterClass
   public void afterClass() {
      embeddedCacheManager.stop();
      cacheManager2.stop();
      cacheManager3.stop();
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
   protected AbstractInfinispanSessionRepository createRepository(SpringCache springCache) {
      InfinispanEmbeddedSessionRepository sessionRepository = new InfinispanEmbeddedSessionRepository(springCache);
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
   public void testSavingNewSession() throws Exception {
      super.testSavingNewSession();
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
   public void testExtractingPrincipalWithWrongIndexName() throws Exception {
      super.testExtractingPrincipalWithWrongIndexName();
   }

   @Override
   public void testExtractingPrincipal() throws Exception {
      super.testExtractingPrincipal();
   }

   @Override
   public void testUpdatingTTLOnAccessingData() throws Exception {
      super.testUpdatingTTLOnAccessingData();
   }
}
