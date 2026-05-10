package org.infinispan.configuration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.test.AbstractInfinispanTest;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "configuration.HashConfigurationBuilderTest")
public class HashConfigurationBuilderTest extends AbstractInfinispanTest {

   public void testNumOwners() {
      ConfigurationBuilder cb = new ConfigurationBuilder();
      cb.clustering().cacheMode(CacheMode.DIST_SYNC);
      cb.clustering().hash().numOwners(5);
      Configuration c = cb.build();
      assertEquals(5, c.clustering().hash().numOwners());

      try {
         cb.clustering().hash().numOwners(0);
         fail("IllegalArgumentException expected");
      } catch (IllegalArgumentException ignore) {
      }
   }

   public void testNumSegments() {
      ConfigurationBuilder cb = new ConfigurationBuilder();
      cb.clustering().cacheMode(CacheMode.DIST_SYNC);
      cb.clustering().hash().numSegments(5);

      Configuration c = cb.build();
      assertEquals(5, c.clustering().hash().numSegments());

      try {
         cb.clustering().hash().numSegments(0);
         fail("IllegalArgumentException expected");
      } catch (IllegalArgumentException ignore) {
      }
   }
}
