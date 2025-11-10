package org.infinispan.eviction.impl;

import java.util.List;

import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.test.AbstractInfinispanTest;
import org.testng.annotations.Factory;
import org.testng.annotations.Test;

@Test (groups = "functional", testName = "eviction.impl.EvictionAccessTest")
public class EvictionAccessTest extends AbstractInfinispanTest {
   private final boolean simpleCache;

   public EvictionAccessTest(boolean simpleCache) {
      this.simpleCache = simpleCache;
   }

   @Factory
   public static Object[] factory() {
      return new Object[] {
            new EvictionAccessTest(true),
            new EvictionAccessTest(false),
      };
   }

   @Override
   protected String parameters() {
      return "[SIMPLE=" + simpleCache + "]";
   }

   public void testAccess() {
      Configuration config = new ConfigurationBuilder()
            .clustering().cacheMode(CacheMode.LOCAL).simpleCache(simpleCache)
            .memory().maxCount(3)
            .build();
      DefaultCacheManager cacheManager = new DefaultCacheManager();
      cacheManager.defineConfiguration("foo", config);
      Cache<String, String> cache = cacheManager.getCache("foo");

      cache.put("1", "1");
      cache.put("2", "2");
      cache.put("3", "3");
      cache.get("3");
      cache.get("3");
      cache.get("3");
      cache.put("4", "4");
      cache.get("4");
      cache.get("4");
      cache.get("4");
      cache.put("5", "5");
      cache.put("6", "6");

      List<String> cacheContents = cache.keySet().stream().toList();
      MatcherAssert.assertThat(cacheContents, Matchers.hasItems("3", "4", "6"));
   }
}
