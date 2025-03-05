package org.infinispan.spring.embedded.provider;

import org.infinispan.commons.IllegalLifecycleStateException;
import org.infinispan.commons.test.Exceptions;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.spring.common.InfinispanTestExecutionListener;
import org.infinispan.spring.common.provider.SpringCache;
import org.infinispan.test.CacheManagerCallable;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.springframework.cache.Cache;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Collection;

import static org.infinispan.test.TestingUtil.withCacheManager;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertNotSame;
import static org.testng.AssertJUnit.assertSame;
import static org.testng.AssertJUnit.assertTrue;

/**
 * <p>
 * Test {@link SpringEmbeddedCacheManager}.
 * </p>
 *
 * @author Olaf Bergner
 * @author Marius Bogoevici
 *
 */
@Test(testName = "spring.embedded.provider.SpringEmbeddedCacheManagerTest", groups = "unit")
@ContextConfiguration(classes = BasicConfiguration.class)
@TestExecutionListeners(value = InfinispanTestExecutionListener.class, mergeMode =  TestExecutionListeners.MergeMode.MERGE_WITH_DEFAULTS)
public class SpringEmbeddedCacheManagerTest extends AbstractTestNGSpringContextTests {

   private static final String CACHE_NAME_FROM_CONFIGURATION_FILE = "asyncCache";

   private static final String NAMED_ASYNC_CACHE_CONFIG_LOCATION = "named-async-cache.xml";

   /**
    * Test method for
    * {@link SpringEmbeddedCacheManager#SpringEmbeddedCacheManager(EmbeddedCacheManager)}
    * .
    */
   @Test(expectedExceptions = IllegalArgumentException.class)
   public final void springEmbeddedCacheManagerConstructorShouldRejectNullEmbeddedCacheManager() {
      new SpringEmbeddedCacheManager(null);
   }

   /**
    * Test method for
    * {@link SpringEmbeddedCacheManager#getCache(String)}.
    *
    * @throws IOException
    */
   @Test
   public final void getCacheShouldReturnTheCacheHavingTheProvidedName() throws IOException {
      final EmbeddedCacheManager nativeCacheManager = TestCacheManagerFactory.fromStream(
            SpringEmbeddedCacheManagerTest.class
                  .getResourceAsStream(NAMED_ASYNC_CACHE_CONFIG_LOCATION));
      final SpringEmbeddedCacheManager objectUnderTest = new SpringEmbeddedCacheManager(
            nativeCacheManager);

      final Cache cacheExpectedToHaveTheProvidedName = objectUnderTest
            .getCache(CACHE_NAME_FROM_CONFIGURATION_FILE);

      assertEquals(
            "getCache("
                  + CACHE_NAME_FROM_CONFIGURATION_FILE
                  + ") should have returned the cache having the provided name. However, the cache returned has a different name.",
            CACHE_NAME_FROM_CONFIGURATION_FILE, cacheExpectedToHaveTheProvidedName.getName());
      nativeCacheManager.stop();
   }

   /**
    * Test method for
    * {@link SpringEmbeddedCacheManager#getCache(String)}.
    *
    * @throws IOException
    */
   @Test
   public final void getCacheShouldReturnACacheAddedAfterCreatingTheSpringEmbeddedCache()
         throws IOException {
      final String nameOfInfinispanCacheAddedLater = "infinispan.cache.addedLater";

      final EmbeddedCacheManager nativeCacheManager = TestCacheManagerFactory.fromStream(
            SpringEmbeddedCacheManagerTest.class
                  .getResourceAsStream(NAMED_ASYNC_CACHE_CONFIG_LOCATION));
      final SpringEmbeddedCacheManager objectUnderTest = new SpringEmbeddedCacheManager(
            nativeCacheManager);
      nativeCacheManager.defineConfiguration(nameOfInfinispanCacheAddedLater, nativeCacheManager.getDefaultCacheConfiguration());

      final org.infinispan.Cache<Object, Object> infinispanCacheAddedLater = nativeCacheManager
            .getCache(nameOfInfinispanCacheAddedLater);

      final Cache springCacheAddedLater = objectUnderTest
            .getCache(nameOfInfinispanCacheAddedLater);

      assertEquals(
            "getCache("
                  + nameOfInfinispanCacheAddedLater
                  + ") should have returned the Spring cache having the Infinispan cache added after creating "
                  + "SpringEmbeddedCacheManager as its underlying native cache. However, the underlying native cache is different.",
            infinispanCacheAddedLater, springCacheAddedLater.getNativeCache());
      nativeCacheManager.stop();
   }

   /**
    * Test method for
    * {@link SpringEmbeddedCacheManager#getCacheNames()}.
    *
    * @throws IOException
    */
   @Test
   public final void getCacheNamesShouldReturnAllCachesDefinedInConfigurationFile()
         throws IOException {
      final EmbeddedCacheManager nativeCacheManager = TestCacheManagerFactory.fromStream(
            SpringEmbeddedCacheManagerTest.class
                  .getResourceAsStream(NAMED_ASYNC_CACHE_CONFIG_LOCATION));
      final SpringEmbeddedCacheManager objectUnderTest = new SpringEmbeddedCacheManager(
            nativeCacheManager);

      final Collection<String> cacheNames = objectUnderTest.getCacheNames();

      assertTrue(
            "SpringEmbeddedCacheManager should load all named caches found in the configuration file of the wrapped "
                  + "native cache manager. However, it does not know about the cache named "
                  + CACHE_NAME_FROM_CONFIGURATION_FILE
                  + " defined in said configuration file.",
            cacheNames.contains(CACHE_NAME_FROM_CONFIGURATION_FILE));
      nativeCacheManager.stop();
   }

   /**
    * Test method for {@link SpringEmbeddedCacheManager#stop()}.
    *
    * @throws IOException
    */
   @Test
   public final void stopShouldStopTheNativeEmbeddedCacheManager() {
      withCacheManager(new CacheManagerCallable(TestCacheManagerFactory.createCacheManager(new ConfigurationBuilder())) {
         @Override
         public void call() {
            cm.getCache(); // Implicitly starts EmbeddedCacheManager
            final SpringEmbeddedCacheManager objectUnderTest = new SpringEmbeddedCacheManager(
                  cm);

            objectUnderTest.stop();

            assertEquals("Calling stop() on SpringEmbeddedCacheManager should stop the enclosed "
                               + "Infinispan EmbeddedCacheManager. However, it is still running.",
                         ComponentStatus.TERMINATED, cm.getStatus());
         }
      });
   }

   /**
    * Test method for
    * {@link SpringEmbeddedCacheManager#getNativeCacheManager()} ()}.
    *
    * @throws IOException
    */
   @Test
   public final void getNativeCacheShouldReturnTheEmbeddedCacheManagerSuppliedAtConstructionTime() {
      withCacheManager(new CacheManagerCallable(TestCacheManagerFactory.createCacheManager()) {
         @Override
         public void call() {
            final SpringEmbeddedCacheManager objectUnderTest = new SpringEmbeddedCacheManager(cm);

            final EmbeddedCacheManager nativeCacheManagerReturned = objectUnderTest.getNativeCacheManager();

            assertSame(
                  "getNativeCacheManager() should have returned the EmbeddedCacheManager supplied at construction time. However, it retuned a different one.",
                  cm, nativeCacheManagerReturned);
         }
      });
   }

   @Test
   public final void getCacheShouldReturnSameInstanceForSameName() {
      withCacheManager(new CacheManagerCallable(TestCacheManagerFactory.createCacheManager()) {
         @Override
         public void call() {
            // Given
            cm.defineConfiguration("same", new ConfigurationBuilder().build());
            final SpringEmbeddedCacheManager objectUnderTest = new SpringEmbeddedCacheManager(cm);
            final String sameCacheName = "same";

            // When
            final SpringCache firstObtainedSpringCache = objectUnderTest.getCache(sameCacheName);
            final SpringCache secondObtainedSpringCache = objectUnderTest.getCache(sameCacheName);

            // Then
            assertSame(
                    "getCache() should have returned the same SpringCache instance for the same name",
                    firstObtainedSpringCache, secondObtainedSpringCache);
         }
      });
   }

   @Test
   public final void getCacheShouldReturnDifferentInstancesForDifferentNames() {
      withCacheManager(new CacheManagerCallable(TestCacheManagerFactory.createCacheManager()) {
         @Override
         public void call() {
            // Given
            cm.defineConfiguration("thisCache", new ConfigurationBuilder().build());
            cm.defineConfiguration("thatCache", new ConfigurationBuilder().build());
            final SpringEmbeddedCacheManager objectUnderTest = new SpringEmbeddedCacheManager(cm);
            final String firstCacheName = "thisCache";
            final String secondCacheName = "thatCache";

            // When
            final SpringCache firstObtainedSpringCache = objectUnderTest.getCache(firstCacheName);
            final SpringCache secondObtainedSpringCache = objectUnderTest.getCache(secondCacheName);

            // Then
            assertNotSame(
                    "getCache() should have returned different SpringCache instances for different names",
                    firstObtainedSpringCache, secondObtainedSpringCache);
         }
      });
   }

   @Test
   public final void getCacheThrowsExceptionForSameNameAfterLifecycleStop() {
      withCacheManager(new CacheManagerCallable(TestCacheManagerFactory.createCacheManager()) {
         @Override
         public void call() {
            // Given
            cm.defineConfiguration("same", new ConfigurationBuilder().build());
            final SpringEmbeddedCacheManager objectUnderTest = new SpringEmbeddedCacheManager(cm);
            final String sameCacheName = "same";

            // When
            final SpringCache obtainedSpringCache = objectUnderTest.getCache(sameCacheName);

            assertNotNull(
                    "getCache() should have returned a SpringCache instance",
                    obtainedSpringCache
            );

            // Given
            objectUnderTest.stop();

            // Then
            Exceptions.expectException(IllegalLifecycleStateException.class, () -> objectUnderTest.getCache(sameCacheName));
         }
      });
   }
}
