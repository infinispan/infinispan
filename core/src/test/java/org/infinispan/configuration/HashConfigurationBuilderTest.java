package org.infinispan.configuration;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.distribution.ch.impl.SyncConsistentHashFactory;
import org.infinispan.test.AbstractInfinispanTest;
import org.testng.Assert;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "configuration.HashConfigurationBuilderTest")
public class HashConfigurationBuilderTest extends AbstractInfinispanTest {

   public void testNumOwners() {
      ConfigurationBuilder cb = new ConfigurationBuilder();
      cb.clustering().cacheMode(CacheMode.DIST_SYNC);
      cb.clustering().hash().numOwners(5);
      Configuration c = cb.build();
      Assert.assertEquals(5, c.clustering().hash().numOwners());

      try {
         cb.clustering().hash().numOwners(0);
         Assert.fail("IllegalArgumentException expected");
      } catch (IllegalArgumentException e) {
      }
   }

   public void testNumSegments() {
      ConfigurationBuilder cb = new ConfigurationBuilder();
      cb.clustering().cacheMode(CacheMode.DIST_SYNC);
      cb.clustering().hash().numSegments(5);

      Configuration c = cb.build();
      Assert.assertEquals(5, c.clustering().hash().numSegments());

      try {
         cb.clustering().hash().numSegments(0);
         Assert.fail("IllegalArgumentException expected");
      } catch (IllegalArgumentException e) {
      }
   }

   public void testConsistentHashFactory() {
      ConfigurationBuilder cb = new ConfigurationBuilder();
      cb.clustering().cacheMode(CacheMode.DIST_SYNC);

      Configuration c = cb.build();
      Assert.assertNull(c.clustering().hash().consistentHashFactory());

      SyncConsistentHashFactory consistentHashFactory = new SyncConsistentHashFactory();
      cb.clustering().hash().consistentHashFactory(consistentHashFactory);
      c = cb.build();
      Assert.assertSame(c.clustering().hash().consistentHashFactory(), consistentHashFactory);
   }
}
