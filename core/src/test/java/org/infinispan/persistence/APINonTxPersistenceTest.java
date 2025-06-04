package org.infinispan.persistence;

import org.infinispan.api.APINonTxTest;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.persistence.dummy.DummyInMemoryStoreConfigurationBuilder;
import org.testng.annotations.Test;

/**
 * Test that ensure that when persistence is used with an always empty data container that various operations
 * are properly supported
 * @author wburns
 * @since 9.2
 */
@Test(groups = "functional", testName = "persistence.APINonTxPersistenceTest")
public class APINonTxPersistenceTest extends APINonTxTest {
   @Override
   protected void configure(ConfigurationBuilder builder) {
      builder
            .memory().maxCount(0)
            .persistence()
               .addStore(DummyInMemoryStoreConfigurationBuilder.class)
                  .storeName(getClass().getName())
            .purgeOnStartup(true)
            ;
   }

   @Override
   public void testEvict() {
      // Ignoring test as we have nothing to evict
   }
}
