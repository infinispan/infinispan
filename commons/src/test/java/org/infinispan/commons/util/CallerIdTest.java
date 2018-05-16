package org.infinispan.commons.util;

import static org.junit.Assert.assertEquals;

import org.infinispan.commons.jdkspecific.CallerId;
import org.infinispan.commons.test.categories.Java10;
import org.junit.Test;
import org.junit.experimental.categories.Category;

public class CallerIdTest {

   @Test
   @Category(Java10.class)
   public void testCaller() {
      assertEquals(this.getClass(), CallerId.getCallerClass(1));
   }
}
