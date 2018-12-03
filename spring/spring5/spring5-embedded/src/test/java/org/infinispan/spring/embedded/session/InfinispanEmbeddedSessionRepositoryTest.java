package org.infinispan.spring.embedded.session;

import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.spring.common.provider.SpringCache;
import org.infinispan.spring.common.session.AbstractInfinispanSessionRepository;
import org.infinispan.spring.common.session.InfinispanSessionRepositoryTCK;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(testName = "spring.embedded.session.InfinispanEmbeddedSessionRepositoryTest", groups = "unit")
public class InfinispanEmbeddedSessionRepositoryTest extends InfinispanSessionRepositoryTCK {

   private EmbeddedCacheManager embeddedCacheManager;

   @BeforeClass
   public void beforeClass() {
      embeddedCacheManager = TestCacheManagerFactory.createCacheManager();
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
