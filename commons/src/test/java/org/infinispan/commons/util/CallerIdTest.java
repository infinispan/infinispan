package org.infinispan.commons.util;

import static org.junit.Assert.assertEquals;

import org.infinispan.commons.jdkspecific.CallerId;
import org.junit.Test;

public class CallerIdTest {

   @Test
   public void testCaller() {
      assertEquals(this.getClass(), CallerId.getCallerClass(1));
   }
}
