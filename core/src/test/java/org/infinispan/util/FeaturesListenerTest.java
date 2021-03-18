package org.infinispan.util;

import org.infinispan.test.AbstractCacheTest;
import org.infinispan.test.AbstractInfinispanTest;
import org.testng.annotations.Test;

@Test(testName = "util.FeaturesListenerTest", groups = "functional")
@AbstractInfinispanTest.FeatureCondition(feature = "A")
public class FeaturesListenerTest extends AbstractCacheTest {

   // this test always be skipped because the feature A is present in the test
   @Test
   public void junitFeatureListenerTest() {
      throw new IllegalStateException("Cannot run.");
   }
}
