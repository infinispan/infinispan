package org.horizon.eviction;

import org.testng.annotations.Test;

@Test(groups = "functional", testName = "eviction.LRUEvictionFunctionalTest")
public class LRUEvictionFunctionalTest extends BaseEvictionFunctionalTest {

   protected EvictionStrategy getEvictionStrategy() {
      return EvictionStrategy.LRU;
   }

}