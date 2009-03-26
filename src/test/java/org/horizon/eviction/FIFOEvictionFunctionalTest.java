package org.horizon.eviction;

import org.testng.annotations.Test;

@Test(groups = "functional", testName = "eviction.FIFOEvictionFunctionalTest")
public class FIFOEvictionFunctionalTest extends BaseEvictionFunctionalTest {

   protected EvictionStrategy getEvictionStrategy() {
      return EvictionStrategy.FIFO;
   }
   
}
