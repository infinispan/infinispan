package org.infinispan.interceptors;

import static org.testng.AssertJUnit.*;
import static org.infinispan.test.TestingUtil.withCacheManager;

import org.infinispan.Cache;
import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.InterceptorConfiguration.Position;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.CacheManagerCallable;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "interceptors.CustomInterceptorTest")
public class CustomInterceptorTest extends AbstractInfinispanTest {

   public void testCustomInterceptorProperties() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.customInterceptors().addInterceptor().interceptor(new FooInterceptor()).position(Position.FIRST).addProperty("foo", "bar");
      withCacheManager(new CacheManagerCallable(
            TestCacheManagerFactory.createCacheManager(builder)) {
         @Override
         public void call() {
            final Cache<Object,Object> cache = cm.getCache();
            CommandInterceptor i = cache.getAdvancedCache().getInterceptorChain().get(0);
            assertTrue("Expecting FooInterceptor in the interceptor chain", i instanceof FooInterceptor);
            assertEquals("bar", ((FooInterceptor)i).getFoo());
         }
      });
   }

   @Test(expectedExceptions=CacheConfigurationException.class)
   public void testMissingPosition() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.customInterceptors().addInterceptor().interceptor(new FooInterceptor());
      TestCacheManagerFactory.createCacheManager(builder);
   }

}
