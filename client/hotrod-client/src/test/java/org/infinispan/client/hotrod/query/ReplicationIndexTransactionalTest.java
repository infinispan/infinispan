package org.infinispan.client.hotrod.query;

import org.testng.annotations.Test;

/**
 * Test indexing during state transfer with a transactional cache.
 */
@Test(groups = "functional", testName = "client.hotrod.query.ReplicationIndexTransactionalTest")
public class ReplicationIndexTransactionalTest extends ReplicationIndexTest {

   @Override
   protected boolean isTransactional() {
      return true;
   }
}
