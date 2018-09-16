package org.infinispan.query.distributed;

import org.testng.annotations.Test;

/**
 * Similar to MultiNodeDistributedTest, but using a replicated configuration both for
 * the indexed cache and for the storage of the index data.
 *
 * @author Sanne Grinovero &lt;sanne@hibernate.org&gt; (C) 2012 Red Hat Inc.
 */
@Test(groups = "functional", testName = "query.distributed.MultiNodeReplicatedTest")
public class MultiNodeReplicatedTest extends MultiNodeDistributedTest {

   @Override
   protected String getConfigurationResourceName() {
      return "dynamic-indexing-replication.xml";
   }

}
