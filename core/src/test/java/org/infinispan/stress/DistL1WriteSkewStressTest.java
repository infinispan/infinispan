package org.infinispan.stress;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.testng.annotations.Test;

import java.util.concurrent.TimeUnit;

/**
 * @author Pedro Ruivo
 * @since 7.0
 */
@Test(testName = "stress.DistL1WriteSkewStressTest", groups = "stress")
public class DistL1WriteSkewStressTest extends DistWriteSkewStressTest {

   @Override
   protected void decorate(ConfigurationBuilder builder) {
      // Enable L1
      builder.clustering().l1().enable();
      builder.clustering().sync().replTimeout(100, TimeUnit.MINUTES);
   }
}
