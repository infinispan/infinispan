package org.horizon.eviction.algorithms.mru;

import org.horizon.eviction.algorithms.BaseAlgorithmTest;
import org.testng.annotations.Test;

@Test(groups = "unit", testName = "eviction.algorithms.mru.MruAlgorithmTest")
public class MruAlgorithmTest extends BaseAlgorithmTest {
   protected MRUAlgorithmConfig getNewEvictionAlgorithmConfig() {
      return new MRUAlgorithmConfig();
   }

   @Override
   protected boolean reverseOrder() {
      return true;
   }
}
