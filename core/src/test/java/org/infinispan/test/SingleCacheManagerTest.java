package org.infinispan.test;

import org.infinispan.Cache;
import org.infinispan.config.Configuration;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Base class for tests that operate on a single (most likely local) cache instance. This operates similar to {@link
 * org.infinispan.test.MultipleCacheManagersTest}, but on only once CacheManager.
 *
 * @author Mircea.Markus@jboss.com
 * @see org.infinispan.test.MultipleCacheManagersTest
 */
@Test
public abstract class SingleCacheManagerTest extends AbstractCacheTest {

   protected EmbeddedCacheManager cacheManager;
   protected Cache<Object, Object> cache;

   protected void setup() throws Exception {
      cacheManager = createCacheManager();
      if (cache == null) cache = cacheManager.getCache();
   }

   protected void teardown() {
      TestingUtil.killCacheManagers(cacheManager);
      cache = null;
      cacheManager = null;
   }

   @BeforeClass()
   protected void createBeforeClass() throws Exception {
      try {
         if (cleanup == CleanupPhase.AFTER_TEST) setup();
      } catch (Exception e) {
         log.error("Unexpected!", e);
         throw e;
      }
   }

   @BeforeMethod
   protected void createBeforeMethod() throws Exception {
      try {
         if (cleanup == CleanupPhase.AFTER_METHOD) setup();
      } catch (Exception e) {
         log.error("Unexpected!", e);
         throw e;
      }
   }
   
   @AfterClass(alwaysRun=true)
   protected void destroyAfterClass() {
      try {
         if (cleanup == CleanupPhase.AFTER_TEST) teardown();
      } catch (Exception e) {
         log.error("Unexpected!", e);
      }
   }

   @AfterMethod(alwaysRun=true)
   protected void destroyAfterMethod() {
      if (cleanup == CleanupPhase.AFTER_METHOD) teardown();
   }

   @AfterMethod(alwaysRun=true)
   protected void clearContent() {
      if (cleanup == CleanupPhase.AFTER_TEST) TestingUtil.clearContent(cacheManager);
   }

   protected Configuration getDefaultStandaloneConfig(boolean transactional) {
      return TestCacheManagerFactory.getDefaultConfiguration(transactional);
   }

   protected abstract EmbeddedCacheManager createCacheManager() throws Exception;
}
