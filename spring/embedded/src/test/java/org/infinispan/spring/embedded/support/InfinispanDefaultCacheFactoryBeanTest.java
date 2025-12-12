package org.infinispan.spring.embedded.support;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.spring.common.InfinispanTestExecutionListener;
import org.infinispan.spring.embedded.InfinispanDefaultCacheFactoryBean;
import org.infinispan.spring.embedded.provider.BasicConfiguration;
import org.infinispan.test.CacheManagerCallable;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.AssertJUnit;
import org.testng.annotations.Test;

import static org.testng.AssertJUnit.assertEquals;

/**
 * <p>
 * Test {@link InfinispanDefaultCacheFactoryBean}.
 * </p>
 *
 * @author Olaf Bergner
 *
 */
@Test(testName = "spring.embedded.support.InfinispanDefaultCacheFactoryBeanTest", groups = "unit")
@ContextConfiguration(classes = BasicConfiguration.class)
@TestExecutionListeners(value = InfinispanTestExecutionListener.class, mergeMode =  TestExecutionListeners.MergeMode.MERGE_WITH_DEFAULTS)
public class InfinispanDefaultCacheFactoryBeanTest extends AbstractTestNGSpringContextTests {

   /**
    * Test method for
    * {@link InfinispanDefaultCacheFactoryBean#afterPropertiesSet()}.
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
    * {@link InfinispanDefaultCacheFactoryBean#getObject()}.
    */
   @Test
   public final void infinispanDefaultCacheFactoryBeanShouldProduceANonNullInfinispanCache() {
      final InfinispanDefaultCacheFactoryBean<Object, Object> objectUnderTest = new InfinispanDefaultCacheFactoryBean<Object, Object>();
      TestingUtil.withCacheManager(new CacheManagerCallable(TestCacheManagerFactory.createCacheManager(new ConfigurationBuilder())) {
         @Override
         public void call() {
            try {
               objectUnderTest.setInfinispanCacheContainer(cm);
               objectUnderTest.afterPropertiesSet();

               final Cache<Object, Object> cache = objectUnderTest.getObject();

               AssertJUnit.assertNotNull(
                       "InfinispanDefaultCacheFactoryBean should have produced a proper Infinispan cache. "
                               + "However, it produced a null Infinispan cache.", cache);
               objectUnderTest.destroy();
            } catch (Exception e) {
               throw new RuntimeException(e);
            }
         }
      });
   }

   /**
    * Test method for
    * {@link InfinispanDefaultCacheFactoryBean#getObjectType()}.
    */
   @Test
   public final void getObjectTypeShouldReturnTheMostDerivedTypeOfTheProducedInfinispanCache() {
      final InfinispanDefaultCacheFactoryBean<Object, Object> objectUnderTest = new InfinispanDefaultCacheFactoryBean<Object, Object>();
      TestingUtil.withCacheManager(new CacheManagerCallable(TestCacheManagerFactory.createCacheManager(new ConfigurationBuilder())) {
         @Override
         public void call() {
            try {
               objectUnderTest.setInfinispanCacheContainer(cm);
               objectUnderTest.afterPropertiesSet();

               assertEquals(
                       "getObjectType() should have returned the produced Infinispan cache's most derived type. "
                               + "However, it returned a more generic type.", objectUnderTest.getObject()
                               .getClass(), objectUnderTest.getObjectType());
               objectUnderTest.destroy();
            } catch (Exception e) {
               throw new RuntimeException(e);
            }
         }
      });

   }

   /**
    * Test method for
    * {@link InfinispanDefaultCacheFactoryBean#isSingleton()}.
    */
   @Test
   public final void infinispanDefaultCacheFactoryBeanShouldDeclareItselfToBeSingleton() {
      final InfinispanDefaultCacheFactoryBean<Object, Object> objectUnderTest = new InfinispanDefaultCacheFactoryBean<Object, Object>();

      AssertJUnit.assertTrue(
              "InfinispanDefaultCacheFactoryBean should declare itself to produce a singleton. However, it didn't.",
              objectUnderTest.isSingleton());
   }

   /**
    * Test method for
    * {@link InfinispanDefaultCacheFactoryBean#destroy()}.
    */
   @Test
   public final void infinispanDefaultCacheFactoryBeanShouldStopTheCreatedInfinispanCacheWhenItIsDestroyed() {
      final InfinispanDefaultCacheFactoryBean<Object, Object> objectUnderTest = new InfinispanDefaultCacheFactoryBean<Object, Object>();
      TestingUtil.withCacheManager(new CacheManagerCallable(TestCacheManagerFactory.createCacheManager(new ConfigurationBuilder())) {
         @Override
         public void call() {
            try {
               objectUnderTest.setInfinispanCacheContainer(cm);
               objectUnderTest.afterPropertiesSet();

               final Cache<Object, Object> cache = objectUnderTest.getObject();
               objectUnderTest.destroy();

               AssertJUnit.assertEquals(
                       "InfinispanDefaultCacheFactoryBean should have stopped the created Infinispan cache when being destroyed. "
                               + "However, the created Infinispan is not yet terminated.",
                       ComponentStatus.TERMINATED, cache.getStatus());
            } catch (Exception e) {
               throw new RuntimeException(e);
            }
         }
      });
   }
}
