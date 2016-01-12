package org.infinispan.interceptors;

import org.infinispan.Cache;
import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.InterceptorConfiguration.Position;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.CacheManagerCallable;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

import java.util.List;

import static org.infinispan.test.TestingUtil.withCacheManager;
import static org.testng.AssertJUnit.*;

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

   public void testLastInterceptor() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.customInterceptors().addInterceptor().position(Position.LAST).interceptor(new FooInterceptor());
      final EmbeddedCacheManager cacheManager = TestCacheManagerFactory.createCacheManager();
      cacheManager.defineConfiguration("interceptors", builder.build());
      withCacheManager(new CacheManagerCallable(cacheManager) {
         @Override
         public void call() {
            List<CommandInterceptor> interceptorChain = cacheManager.getCache("interceptors").getAdvancedCache().getInterceptorChain();
            assertEquals(interceptorChain.get(interceptorChain.size() - 2).getClass(), FooInterceptor.class);
         }
      });
   }

   public void testOtherThanFirstOrLastInterceptor() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.customInterceptors().addInterceptor().position(Position.OTHER_THAN_FIRST_OR_LAST).interceptor(new FooInterceptor());
      final EmbeddedCacheManager cacheManager = TestCacheManagerFactory.createCacheManager();
      cacheManager.defineConfiguration("interceptors", builder.build());
      withCacheManager(new CacheManagerCallable(cacheManager) {
         @Override
         public void call() {
            List<CommandInterceptor> interceptorChain = cacheManager.getCache("interceptors").getAdvancedCache().getInterceptorChain();
            assertEquals(interceptorChain.get(1).getClass(), FooInterceptor.class);
         }
      });
   }

   public void testLastInterceptorDefaultCache() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      final FooInterceptor interceptor = new FooInterceptor();
      builder.customInterceptors().addInterceptor().position(Position.LAST).interceptor(interceptor);
      final EmbeddedCacheManager cacheManager = TestCacheManagerFactory.createCacheManager(builder);
      withCacheManager(new CacheManagerCallable(cacheManager) {
         @Override
         public void call() {
            List<CommandInterceptor> interceptorChain = cacheManager.getCache().getAdvancedCache().getInterceptorChain();
            assertEquals(interceptorChain.get(interceptorChain.size() - 2).getClass(), FooInterceptor.class);
            assertFalse(interceptor.putInvoked.get());
            cacheManager.getCache().put("k", "v");
            assertEquals("v", cacheManager.getCache().get("k"));
            assertTrue(interceptor.putInvoked.get());
         }
      });
   }

}
