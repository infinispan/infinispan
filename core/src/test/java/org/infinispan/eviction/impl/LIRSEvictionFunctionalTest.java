package org.infinispan.eviction.impl;

import org.infinispan.eviction.EvictionStrategy;
import org.testng.annotations.Test;

@Test(groups = {"functional", "smoke"}, testName = "eviction.LIRSEvictionFunctionalTest")
public class LIRSEvictionFunctionalTest extends BaseEvictionFunctionalTest {

   protected EvictionStrategy getEvictionStrategy() {
      return EvictionStrategy.LIRS;
   }
}