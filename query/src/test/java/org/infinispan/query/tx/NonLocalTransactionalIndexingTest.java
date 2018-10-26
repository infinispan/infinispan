package org.infinispan.query.tx;

import org.testng.annotations.Test;

/**
 * Test to make sure indexLocalOnly=false behaves the same with or without
 * transactions enabled.
 * See also ISPN-2467 and subclasses.
 *
 * @author Sanne Grinovero &lt;sanne@hibernate.org&gt; (C) 2012 Red Hat Inc.
 */
@Test(groups = "functional", testName = "query.tx.NonLocalTransactionalIndexingTest")
public class NonLocalTransactionalIndexingTest extends NonLocalIndexingTest {

   @Override
   protected boolean transactionsEnabled() {
      return true;
   }

}
