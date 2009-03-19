package org.horizon.eviction.algorithms.fifo;

import org.horizon.eviction.algorithms.BaseAlgorithmTest;
import org.testng.annotations.Test;

@Test(groups = "unit", testName = "eviction.algorithms.fifo.FifoAlgorithmTest")
public class FifoAlgorithmTest extends BaseAlgorithmTest {
   protected FIFOAlgorithmConfig getNewEvictionAlgorithmConfig() {
      return new FIFOAlgorithmConfig();
   }
}
