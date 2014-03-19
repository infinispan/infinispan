package org.infinispan.eviction.impl;

import org.infinispan.eviction.EvictionStrategy;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "eviction.UNORDEREDEvictionFunctionalTest")
public class UNORDEREDEvictionFunctionalTest extends BaseEvictionFunctionalTest {

   protected EvictionStrategy getEvictionStrategy() {
      return EvictionStrategy.UNORDERED;
   }
}