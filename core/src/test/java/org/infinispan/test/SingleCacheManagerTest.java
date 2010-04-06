package org.infinispan.test;

import org.infinispan.Cache;
import org.infinispan.config.Configuration;
import org.infinispan.manager.CacheManager;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
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

   private static Log log = LogFactory.getLog(SingleCacheManagerTest.class);

   protected CacheManager cacheManager;
   protected Cache<Object, Object> cache;

   @BeforeClass()
   protected void createBeforeClass() throws Exception {
      try {
         if (cleanup == CleanupPhase.AFTER_TEST) cacheManager = createCacheManager();
      } catch (Exception e) {
         log.error("Unexpected!", e);
         throw e;
      }
   }

   @BeforeMethod
   protected void createBeforeMethod() throws Exception {
      try {
         if (cleanup == CleanupPhase.AFTER_METHOD) cacheManager = createCacheManager();
      } catch (Exception e) {
         log.error("Unexpected!", e);
         throw e;
      }
   }
   
   @AfterClass(alwaysRun=true)
   protected void destroyAfterClass() {
      try {
         if (cleanup == CleanupPhase.AFTER_TEST) TestingUtil.killCacheManagers(cacheManager);
      } catch (Exception e) {
         log.error("Unexpected!", e);
      }
   }

   @AfterMethod(alwaysRun=true)
   protected void destroyAfterMethod() {
      if (cleanup == CleanupPhase.AFTER_METHOD) TestingUtil.killCacheManagers(cacheManager);
   }

   @AfterMethod(alwaysRun=true)
   protected void clearContent() {
      if (cleanup == CleanupPhase.AFTER_TEST) TestingUtil.clearContent(cacheManager);
   }

   protected Configuration getDefaultStandaloneConfig(boolean transactional) {
      return TestCacheManagerFactory.getDefaultConfiguration(transactional);
   }

   protected abstract CacheManager createCacheManager() throws Exception;
}
