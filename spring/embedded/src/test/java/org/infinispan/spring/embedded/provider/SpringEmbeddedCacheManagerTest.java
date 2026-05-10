package org.infinispan.spring.embedded.provider;

import static org.infinispan.test.TestingUtil.withCacheManager;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.Collection;

import org.infinispan.commons.IllegalLifecycleStateException;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.spring.common.InfinispanTestExecutionListener;
import org.infinispan.spring.common.provider.SpringCache;
import org.infinispan.test.CacheManagerCallable;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.testing.Exceptions;
import org.springframework.cache.Cache;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.Test;

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
@TestExecutionListeners(value = InfinispanTestExecutionListener.class, mergeMode = TestExecutionListeners.MergeMode.MERGE_WITH_DEFAULTS)
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
            CACHE_NAME_FROM_CONFIGURATION_FILE, cacheExpectedToHaveTheProvidedName.getName(),
            "getCache("
                  + CACHE_NAME_FROM_CONFIGURATION_FILE
                  + ") should have returned the cache having the provided name. However, the cache returned has a different name.");
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
            infinispanCacheAddedLater, springCacheAddedLater.getNativeCache(),
            "getCache("
                  + nameOfInfinispanCacheAddedLater
                  + ") should have returned the Spring cache having the Infinispan cache added after creating "
                  + "SpringEmbeddedCacheManager as its underlying native cache. However, the underlying native cache is different.");
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
            cacheNames.contains(CACHE_NAME_FROM_CONFIGURATION_FILE),
            "SpringEmbeddedCacheManager should load all named caches found in the configuration file of the wrapped "
                  + "native cache manager. However, it does not know about the cache named "
                  + CACHE_NAME_FROM_CONFIGURATION_FILE
                  + " defined in said configuration file.");
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

            assertEquals(ComponentStatus.TERMINATED, cm.getStatus(),
                  "Calling stop() on SpringEmbeddedCacheManager should stop the enclosed "
                        + "Infinispan EmbeddedCacheManager. However, it is still running.");
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
                  cm, nativeCacheManagerReturned,
                  "getNativeCacheManager() should have returned the EmbeddedCacheManager supplied at construction time. However, it retuned a different one.");
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
                  firstObtainedSpringCache, secondObtainedSpringCache,
                  "getCache() should have returned the same SpringCache instance for the same name");
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
            assertNotSame(firstObtainedSpringCache, secondObtainedSpringCache,
                  "getCache() should have returned different SpringCache instances for different names");
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
                  obtainedSpringCache,
                  "getCache() should have returned a SpringCache instance"
            );

            // Given
            objectUnderTest.stop();

            // Then
            Exceptions.expectException(IllegalLifecycleStateException.class, () -> objectUnderTest.getCache(sameCacheName));
         }
      });
   }
}
