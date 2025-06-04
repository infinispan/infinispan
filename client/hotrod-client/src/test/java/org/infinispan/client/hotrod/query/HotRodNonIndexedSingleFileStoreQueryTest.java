package org.infinispan.client.hotrod.query;

import org.infinispan.commons.test.CommonsTestingUtil;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.SingleFileStoreConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.testng.annotations.Test;

/**
 * @author anistor@redhat.com
 * @since 7.0
 */
@Test(testName = "client.hotrod.query.HotRodNonIndexedSingleFileStoreQueryTest", groups = "functional")
public class HotRodNonIndexedSingleFileStoreQueryTest extends HotRodNonIndexedQueryTest {

   private final String tmpDirectory = CommonsTestingUtil.tmpDirectory(getClass());

   @Override
   protected void setup() throws Exception {
      Util.recursiveFileRemove(tmpDirectory);
      super.setup();
   }

   @Override
   protected void teardown() {
      try {
         super.teardown();
      } finally {
         Util.recursiveFileRemove(tmpDirectory);
      }
   }

   @Override
   protected void configure(GlobalConfigurationBuilder builder) {
      builder.globalState().persistentLocation(tmpDirectory);
   }

   @Override
   protected ConfigurationBuilder getConfigurationBuilder() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.persistence()
            .addStore(SingleFileStoreConfigurationBuilder.class)
            .location(tmpDirectory);

      // ensure the data container contains minimal data so the store will need to be accessed to get the rest
      builder.locking().concurrencyLevel(1).memory().maxCount(1);
      return builder;
   }
}
