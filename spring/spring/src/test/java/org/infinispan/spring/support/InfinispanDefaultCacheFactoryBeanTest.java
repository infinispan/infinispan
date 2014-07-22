package org.infinispan.spring.support;

import org.infinispan.Cache;
import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.test.CacheManagerCallable;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

import static org.infinispan.test.TestingUtil.withCacheManager;
import static org.testng.AssertJUnit.*;

/**
 * <p>
 * Test {@link InfinispanDefaultCacheFactoryBean}.
 * </p>
 *
 * @author <a href="mailto:olaf DOT bergner AT gmx DOT de">Olaf Bergner</a>
 *
 */
@Test(testName = "spring.support.InfinispanDefaultCacheFactoryBeanTest", groups = "unit")
public class InfinispanDefaultCacheFactoryBeanTest {

   /**
    * Test method for
    * {@link org.infinispan.spring.support.InfinispanDefaultCacheFactoryBean#afterPropertiesSet()}.
    *
    * @throws Exception
    */
   @Test(expectedExceptions = IllegalStateException.class)
   public final void afterPropertiesSetShouldThrowAnIllegalStateExceptionIfNoCacheContainerHasBeenSet()
         throws Exception {
      final InfinispanDefaultCacheFactoryBean<Object, Object> objectUnderTest = new InfinispanDefaultCacheFactoryBean<Object, Object>();
      objectUnderTest.afterPropertiesSet();
   }

   /**
    * Test method for
    * {@link org.infinispan.spring.support.InfinispanDefaultCacheFactoryBean#getObject()}.
    */
   @Test
   public final void infinispanDefaultCacheFactoryBeanShouldProduceANonNullInfinispanCache() {
      final InfinispanDefaultCacheFactoryBean<Object, Object> objectUnderTest = new InfinispanDefaultCacheFactoryBean<Object, Object>();
      withCacheManager(new CacheManagerCallable(TestCacheManagerFactory.createCacheManager()) {
         @Override
         public void call() throws Exception {
            objectUnderTest.setInfinispanCacheContainer(cm);
            objectUnderTest.afterPropertiesSet();

            final Cache<Object, Object> cache = objectUnderTest.getObject();

            assertNotNull(
                  "InfinispanDefaultCacheFactoryBean should have produced a proper Infinispan cache. "
                        + "However, it produced a null Infinispan cache.", cache);
            objectUnderTest.destroy();
         }
      });
   }

   /**
    * Test method for
    * {@link org.infinispan.spring.support.InfinispanDefaultCacheFactoryBean#getObjectType()}.
    */
   @Test
   public final void getObjectTypeShouldReturnTheMostDerivedTypeOfTheProducedInfinispanCache() {
      final InfinispanDefaultCacheFactoryBean<Object, Object> objectUnderTest = new InfinispanDefaultCacheFactoryBean<Object, Object>();
      withCacheManager(new CacheManagerCallable(TestCacheManagerFactory.createCacheManager()) {
         @Override
         public void call() throws Exception {
            objectUnderTest.setInfinispanCacheContainer(cm);
            objectUnderTest.afterPropertiesSet();

            assertEquals(
                  "getObjectType() should have returned the produced Infinispan cache's most derived type. "
                        + "However, it returned a more generic type.", objectUnderTest.getObject()
                        .getClass(), objectUnderTest.getObjectType());
            objectUnderTest.destroy();
         }
      });

   }

   /**
    * Test method for
    * {@link org.infinispan.spring.support.InfinispanDefaultCacheFactoryBean#isSingleton()}.
    */
   @Test
   public final void infinispanDefaultCacheFactoryBeanShouldDeclareItselfToBeSingleton() {
      final InfinispanDefaultCacheFactoryBean<Object, Object> objectUnderTest = new InfinispanDefaultCacheFactoryBean<Object, Object>();

      assertTrue(
            "InfinispanDefaultCacheFactoryBean should declare itself to produce a singleton. However, it didn't.",
            objectUnderTest.isSingleton());
   }

   /**
    * Test method for
    * {@link org.infinispan.spring.support.InfinispanDefaultCacheFactoryBean#destroy()}.
    */
   @Test
   public final void infinispanDefaultCacheFactoryBeanShouldStopTheCreatedInfinispanCacheWhenItIsDestroyed() {
      final InfinispanDefaultCacheFactoryBean<Object, Object> objectUnderTest = new InfinispanDefaultCacheFactoryBean<Object, Object>();
      withCacheManager(new CacheManagerCallable(TestCacheManagerFactory.createCacheManager()) {
         @Override
         public void call() throws Exception {
            objectUnderTest.setInfinispanCacheContainer(cm);
            objectUnderTest.afterPropertiesSet();

            final Cache<Object, Object> cache = objectUnderTest.getObject();
            objectUnderTest.destroy();

            assertEquals(
                  "InfinispanDefaultCacheFactoryBean should have stopped the created Infinispan cache when being destroyed. "
                        + "However, the created Infinispan is not yet terminated.",
                  ComponentStatus.TERMINATED, cache.getStatus());
         }
      });
   }
}
