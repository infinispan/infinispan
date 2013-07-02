package org.infinispan.query.statetransfer;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.query.test.Person;
import org.testng.annotations.Test;

/**
 * Test that verifies that querying works even after a new node is added and
 * state transfer has provided it with the data belonging to that node.
 *
 * @author Galder Zamarreño
 * @since 5.2
 */
@Test(groups = "functional", testName = "query.statetransfer.StateTransferQueryIndexTest")
public class StateTransferQueryIndexTest extends BaseReIndexingTest {

   @Override
   protected void configureCache(ConfigurationBuilder builder) {
      builder.clustering().stateTransfer().fetchInMemoryState(true);
   }

   public void testQueryAfterAddingNewNode() throws Exception {
      loadCacheEntries(this.<String, Person>caches().get(0));

      // Before adding a node, verify that the query resolves properly
      executeSimpleQuery(this.<String, Person>caches().get(0));

      addNodeCheckingContentsAndQuery();
   }

}
