package org.infinispan.client.hotrod.query.pressure;

public class TransactionalLargePutAllPressureTest extends LargePutAllPressureTest {

   protected boolean useTransactions() {
      return true;
   }

}
