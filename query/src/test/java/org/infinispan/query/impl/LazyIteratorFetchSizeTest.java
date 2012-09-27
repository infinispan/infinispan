package org.infinispan.query.impl;

/**
 *
 */
public class LazyIteratorFetchSizeTest extends LazyIteratorTest {

   @Override
   protected int getFetchSize() {
      return 10;
   }

}
