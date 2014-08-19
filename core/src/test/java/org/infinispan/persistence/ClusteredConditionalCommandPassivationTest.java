package org.infinispan.persistence;

import org.testng.annotations.Test;

/**
 * Tests if the conditional commands correctly fetch the value from cache loader even with the skip cache load/store
 * flags.
 * <p/>
 * The configuration used is a non-tx distributed cache with passivation.
 *
 * @author Pedro Ruivo
 * @since 7.0
 */
@Test(groups = "functional", testName = "persistence.ClusteredConditionalCommandPassivationTest")
public class ClusteredConditionalCommandPassivationTest extends ClusteredConditionalCommandTest {

   public ClusteredConditionalCommandPassivationTest() {
      super(false, true);
   }

}
