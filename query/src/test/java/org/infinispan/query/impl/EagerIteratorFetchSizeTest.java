package org.infinispan.query.impl;

/**
 *
 */
public class EagerIteratorFetchSizeTest extends EagerIteratorTest {

   @Override
   protected int getFetchSize() {
      return 10;
   }

}
