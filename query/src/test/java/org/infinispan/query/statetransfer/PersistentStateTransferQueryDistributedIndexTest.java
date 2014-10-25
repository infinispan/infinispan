package org.infinispan.query.statetransfer;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.Index;
import org.testng.annotations.Test;

/**
 * @author anistor@redhat.com
 * @since 5.2
 */
@Test(groups = "functional", testName = "query.statetransfer.PersistentStateTransferQueryDistributedIndexTest")
public class PersistentStateTransferQueryDistributedIndexTest extends PersistentStateTransferQueryIndexTest {

   @Override
   protected void configureCache(ConfigurationBuilder builder) {
      super.configureCache(builder);

      builder.indexing().index(Index.LOCAL)
            .addProperty("default.indexmanager", "org.infinispan.query.indexmanager.InfinispanIndexManager");
   }
}
