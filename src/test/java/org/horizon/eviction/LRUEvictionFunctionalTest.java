package org.horizon.eviction;

import org.testng.annotations.Test;

@Test(groups = "functional", testName = "eviction.LRUEvictionFunctionalTest", enabled = false)
public class LRUEvictionFunctionalTest extends BaseEvictionFunctionalTest {

   protected EvictionStrategy getEvictionStrategy() {
      return EvictionStrategy.LRU;
   }

}