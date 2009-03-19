package org.horizon.eviction.algorithms.lru;

import org.horizon.eviction.algorithms.BaseAlgorithmTest;
import org.testng.annotations.Test;

@Test(groups = "unit", testName = "eviction.algorithms.lru.LruAlgorithmTest")
public class LruAlgorithmTest extends BaseAlgorithmTest {
   protected LRUAlgorithmConfig getNewEvictionAlgorithmConfig() {
      return new LRUAlgorithmConfig();
   }
}
