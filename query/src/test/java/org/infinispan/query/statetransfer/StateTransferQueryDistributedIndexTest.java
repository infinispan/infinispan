package org.infinispan.query.statetransfer;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.query.helper.SearchConfig;
import org.testng.annotations.Test;

/**
 * @author anistor@redhat.com
 * @since 5.2
 */
@Test(groups = "functional", testName = "query.statetransfer.StateTransferQueryDistributedIndexTest")
public class StateTransferQueryDistributedIndexTest extends StateTransferQueryIndexTest {

   @Override
   protected void configureCache(ConfigurationBuilder builder) {
      super.configureCache(builder);
      builder.indexing().enable()
            .addProperty(SearchConfig.DIRECTORY_TYPE, SearchConfig.HEAP);

   }
}
