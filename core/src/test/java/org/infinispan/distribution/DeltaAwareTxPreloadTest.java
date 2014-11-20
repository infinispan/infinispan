package org.infinispan.distribution;

public class DeltaAwareTxPreloadTest extends BaseDeltaAwarePreloadTest {

   @Override
   boolean isTx() {
      return true;
   }
}
