package org.infinispan.eviction;

import org.testng.annotations.Test;

@Test(groups = "functional", testName = "eviction.LIRSEvictionFunctionalTest")
public class LIRSEvictionFunctionalTest extends BaseEvictionFunctionalTest {

   protected EvictionStrategy getEvictionStrategy() {
      return EvictionStrategy.LIRS;
   }
}