package org.infinispan.client.hotrod.query;

import org.infinispan.commons.equivalence.ByteArrayEquivalence;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.SingleFileStoreConfigurationBuilder;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.Test;

/**
 * @author anistor@redhat.com
 * @since 7.0
 */
@Test(testName = "client.hotrod.query.HotRodNonIndexedSingleFileStoreQueryTest", groups = "functional")
public class HotRodNonIndexedSingleFileStoreQueryTest extends HotRodNonIndexedQueryTest {

   private final String tmpDirectory = TestingUtil.tmpDirectory(getClass());

   @Override
   protected void setup() throws Exception {
      TestingUtil.recursiveFileRemove(tmpDirectory);
      super.setup();
   }

   @Override
   protected void teardown() {
      try {
         super.teardown();
      } finally {
         TestingUtil.recursiveFileRemove(tmpDirectory);
      }
   }

   @Override
   protected ConfigurationBuilder getConfigurationBuilder() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.dataContainer().keyEquivalence(ByteArrayEquivalence.INSTANCE)
            .persistence()
            .addStore(SingleFileStoreConfigurationBuilder.class)
            .location(tmpDirectory);

      // ensure the data container contains minimal data so the store will need to be accessed to get the rest
      builder.locking().concurrencyLevel(1).dataContainer().eviction().maxEntries(1);

      return builder;
   }
}
