package org.infinispan.distribution.rehash;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

/**
 * Tests data loss during state transfer when a single data owner is configured.
 * @author Sanne Grinovero &lt;sanne@infinispan.org&gt; (C) 2011 Red Hat Inc.
 * @author Alex Heneveld
 * @author Manik Surtani
 */
@Test(groups = "functional", testName = "distribution.rehash.DataLossOnJoinOneOwnerTest")
public class DataLossOnJoinOneOwnerTest extends AbstractInfinispanTest {

   private static final String VALUE = DataLossOnJoinOneOwnerTest.class.getName() + "value";
   private static final String KEY = DataLossOnJoinOneOwnerTest.class.getName() + "key";

   EmbeddedCacheManager cm1;
   EmbeddedCacheManager cm2;

   /**
    * It seems that sometimes when a new node joins, existing data is lost.
    * Can not reproduce with numOwners=2.
    */
   public void testDataLossOnJoin() {
      try {
         cm1 = newCM();
         Cache<String, String> c1 = cm1.getCache();
         c1.put(KEY, VALUE);
         hasKey(c1);
         cm2 = newCM();
         Cache<String, String> c2 = cm2.getCache();
         TestingUtil.blockUntilViewsReceived(45000, cm1, cm2);
         hasKey(c1);
         hasKey(c2);
      }
      finally {
         TestingUtil.killCacheManagers(cm1, cm2);
      }
   }

   private void hasKey(Cache<String, String> cache) {
      Object object = cache.get(KEY);
      assert VALUE.equals(object);
   }

   public EmbeddedCacheManager newCM() {
      ConfigurationBuilder c = new ConfigurationBuilder();
      c.clustering().cacheMode(CacheMode.DIST_SYNC)
               .hash().numOwners(1)
            .clustering().l1().disable();
      return TestCacheManagerFactory.createClusteredCacheManager(c);
   }
}
