package org.infinispan.jcache.annotation;

import static org.infinispan.jcache.util.Deployments.baseDeploymentInjectedInterceptors;
import static org.testng.Assert.assertEquals;

import javax.enterprise.inject.Produces;
import javax.inject.Inject;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.eviction.EvictionStrategy;
import org.infinispan.jcache.annotation.greetingservice.GreetingService;
import org.infinispan.manager.EmbeddedCacheManager;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.testng.Arquillian;
import org.jboss.shrinkwrap.api.Archive;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "cdi.test.jcache.annotation.InjectedCacheResultInterceptorTest")
public class InjectedCacheResultInterceptorTest extends Arquillian {

   private static final EvictionStrategy DEFAULT_CACHE_EVICTION_STRATEGY = EvictionStrategy.LRU;

   private static final int DEFAULT_CACHE_MAX_ENTRIES = 4;

   public static final long DEFAULT_CACHE_LIFESPAN = 1000L;

   @Deployment
   public static Archive<?> deployment() {
      return baseDeploymentInjectedInterceptors().addClass(
            InjectedCacheResultInterceptorTest.class).addPackage(
                  GreetingService.class.getPackage());
   }

   @Produces
   public Configuration defaultCacheConfiguration() {
      return new ConfigurationBuilder().expiration()
            .lifespan(DEFAULT_CACHE_LIFESPAN)
            .eviction()
            .strategy(DEFAULT_CACHE_EVICTION_STRATEGY)
            .maxEntries(DEFAULT_CACHE_MAX_ENTRIES).build();
   }

   @Inject
   private GreetingService greetingService;

   @Inject
   private EmbeddedCacheManager embeddedCacheManager;

   @Test
   public void testDefaultCacheManagerInjected() throws Exception {
      final Configuration defaultCacheConfiguration = embeddedCacheManager
            .getDefaultCacheConfiguration();
      assertEquals(defaultCacheConfiguration.expiration().lifespan(), DEFAULT_CACHE_LIFESPAN);
      assertEquals(defaultCacheConfiguration.eviction().strategy(), DEFAULT_CACHE_EVICTION_STRATEGY);
      assertEquals(defaultCacheConfiguration.eviction().maxEntries(), DEFAULT_CACHE_MAX_ENTRIES);

   }

   @Test
   public void testInjectedCacheResultUsingDefaultCacheManager()
         throws Exception {

      final String methodParameter = "Chuck Liddell";
      final String methodResult = greetingService
            .greetWithDefaultCacheConfig(methodParameter);

      final String cacheName = GreetingService.class.getName()
            + ".greetWithDefaultCacheConfig(java.lang.String)";
      final DefaultCacheKey invocationCacheKey = toDefaultCacheKey(methodParameter);
      assertEquals(getDefaultManagerCache(cacheName).get(invocationCacheKey), methodResult);

   }

   @Test
   public void testInjectedCacheResultInvokingTwoDifferentCachesWithDefaultConfiguration()
         throws Exception {
      greetingService.greetWithDefaultCacheConfig("John Rambo");
      greetingService.greetTwoWithDefaultCacheConfig("John Wayne");

   }

   /**
    * See bug https://issues.jboss.org/browse/ISPN-5195.
    *
    * @throws Exception
    *            error
    */
   @Test
   public void testInjectedCacheResult() throws Exception {
      final String name = "Edddie Murphy";
      final String greetOne = greetingService.greet(name);
      final String greetTwo = greetingService.greet(name);
      assertEquals(greetTwo, greetOne);
   }

   private DefaultCacheKey toDefaultCacheKey(final String cacheKey) {
      return new DefaultCacheKey(new String[] { cacheKey });
   }

   private Cache<Object, Object> getDefaultManagerCache(final String cacheName) {
      return embeddedCacheManager.getCache(cacheName);
   }

}