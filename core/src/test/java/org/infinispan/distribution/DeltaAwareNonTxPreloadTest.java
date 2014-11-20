package org.infinispan.distribution;

public class DeltaAwareNonTxPreloadTest extends BaseDeltaAwarePreloadTest {

   @Override
   boolean isTx() {
      return false;
   }
}
