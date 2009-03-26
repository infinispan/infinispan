package org.horizon.test;

import org.horizon.Cache;
import org.horizon.manager.CacheManager;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;

/**
 * Base class for tests that operate on a single (most likely local) cache instance. This operates similar to {@link
 * org.horizon.test.MultipleCacheManagersTest}, but on only once CacheManager.
 *
 * @author Mircea.Markus@jboss.com
 * @see org.horizon.test.MultipleCacheManagersTest
 */
public abstract class SingleCacheManagerTest extends AbstractCacheTest {
   protected CacheManager cacheManager;
   protected Cache cache;

   /**
    * This method will always be called before {@link #createBeforeClass()}.  If you override this, make sure you annotate the
    * overridden method with {@link org.testng.annotations.BeforeClass}.
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
   @BeforeClass(dependsOnMethods = "org.horizon.*.preCreate")
   protected void createBeforeClass() throws Exception {
      if (cleanup == CleanupPhase.AFTER_TEST) cacheManager = createCacheManager();
   }

   @BeforeMethod
   protected void createBeforeMethod() throws Exception {
      if (cleanup == CleanupPhase.AFTER_METHOD) cacheManager = createCacheManager();
   }

   @AfterClass
   protected void destroyAfterClass() {
      if (cleanup == CleanupPhase.AFTER_TEST) TestingUtil.killCacheManagers(cacheManager);
   }

   @AfterMethod
   protected void destroyAfterMethod() {
      if (cleanup == CleanupPhase.AFTER_METHOD) TestingUtil.killCacheManagers(cacheManager);
   }

   @AfterMethod
   protected void clearContent() {
      if (cleanup == CleanupPhase.AFTER_TEST) super.clearContent(cacheManager);
   }

   protected abstract CacheManager createCacheManager() throws Exception;
}
