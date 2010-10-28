package org.infinispan.distribution;

import org.testng.annotations.Test;

@Test(groups = "functional", testName = "distribution.DistSkipRemoteLookupWithoutL1Test")
public class DistSkipRemoteLookupWithoutL1Test extends DistSkipRemoteLookupTest {
   
   public DistSkipRemoteLookupWithoutL1Test() {
      l1CacheEnabled = false;
   }

}
