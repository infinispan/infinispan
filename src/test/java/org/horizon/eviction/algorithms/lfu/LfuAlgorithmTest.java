package org.horizon.eviction.algorithms.lfu;

import org.horizon.eviction.algorithms.BaseAlgorithmTest;
import org.testng.annotations.Test;

@Test(groups = "unit", testName = "eviction.algorithms.lfu.LfuAlgorithmTest")
public class LfuAlgorithmTest extends BaseAlgorithmTest {
   @Override
   protected boolean timeOrderedQueue() {
      return false;
   }

   protected LFUAlgorithmConfig getNewEvictionAlgorithmConfig() {
      return new LFUAlgorithmConfig();
   }
}
