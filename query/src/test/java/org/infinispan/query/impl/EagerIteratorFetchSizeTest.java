package org.infinispan.query.impl;

import org.testng.annotations.Test;

/**
 *
 */
@Test(groups = "functional", testName = "query.impl.EagerIteratorFetchSizeTest")
public class EagerIteratorFetchSizeTest extends EagerIteratorTest {

   @Override
   protected int getFetchSize() {
      return 10;
   }

}
