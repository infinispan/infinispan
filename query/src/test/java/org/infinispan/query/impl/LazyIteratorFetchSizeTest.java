package org.infinispan.query.impl;

import org.testng.annotations.Test;

/**
 *
 */
@Test(groups = "functional", testName = "query.impl.LazyIteratorFetchSizeTest")
public class LazyIteratorFetchSizeTest extends LazyIteratorTest {

   @Override
   protected int getFetchSize() {
      return 10;
   }

}
