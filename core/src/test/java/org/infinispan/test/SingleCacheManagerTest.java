package org.infinispan.test;

import org.infinispan.Cache;
import org.infinispan.config.Configuration;
import org.infinispan.manager.CacheManager;
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
@Test(groups = {"functional", "unit"})
public abstract class SingleCacheManagerTest extends AbstractCacheTest {
   protected CacheManager cacheManager;
   protected Cache cache;

   /**
    * This method will always be called before {@link #createBeforeClass()}.  If you override this, make sure you
    * annotate the overridden method with {@link org.testng.annotations.BeforeClass}.
    *
    * @throws Exception Just in case
    */
   @BeforeClass
   public void preCreate() throws Exception {
      // no op, made for overriding.
   }

   // Due to some weirdness with TestNG, it always appends the package and class name to the method names
   // provided on dependsOnMethods unless it thinks there already is a package.  This does accept regular expressions
   // though so .*. works.  Otherwise it won't detect overridden methods in subclasses.
   @BeforeClass(dependsOnMethods = "org.infinispan.*.preCreate")
   protected void createBeforeClass() throws Exception {
      if (cleanup == CleanupPhase.AFTER_TEST) cacheManager = createCacheManager();
   }

   @BeforeMethod
   protected void createBeforeMethod() throws Exception {
      if (cleanup == CleanupPhase.AFTER_METHOD) cacheManager = createCacheManager();
   }

   @AfterClass(alwaysRun=true)
   protected void destroyAfterClass() {      
      if (cleanup == CleanupPhase.AFTER_TEST) TestingUtil.killCacheManagers(cacheManager);
      cacheManager = null;
      cache = null;
   }

   @AfterMethod(alwaysRun=true)
   protected void destroyAfterMethod() {
      if (cleanup == CleanupPhase.AFTER_METHOD) TestingUtil.killCacheManagers(cacheManager);
   }

   @AfterMethod(alwaysRun=true)
   protected void clearContent() {
      if (cleanup == CleanupPhase.AFTER_TEST) super.clearContent(cacheManager);
   }

   protected Configuration getDefaultStandaloneConfig(boolean transactional) {
      return TestCacheManagerFactory.getDefaultConfiguration(transactional);
   }

   protected abstract CacheManager createCacheManager() throws Exception;
}
